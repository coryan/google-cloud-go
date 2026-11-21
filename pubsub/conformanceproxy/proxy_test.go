// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	gax "github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	otrace "go.opentelemetry.io/otel/trace"
	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var sharedPT = timestamppb.New(time.Unix(1_700_000_000, 123456789))
var stopSubCmd = &proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{}}}

const autoAck, autoNack, autoDrop, interactive, waitBhv, nackBhv = proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE, proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING, proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY

type testEnv struct {
	t      *testing.T
	ctx    context.Context
	client proxypb.PubSubProxyClient
	srv    *proxyServer
	tm     *traceManager
}

func newTestEnv(t *testing.T, timeout time.Duration) *testEnv {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	noErr(t, err, "listen")
	tm, gs := initTracing(), grpc.NewServer(grpc.MaxRecvMsgSize(maxGRPCMessageBytes), grpc.MaxSendMsgSize(maxGRPCMessageBytes))
	ps := newProxyServer(gs, tm)
	proxypb.RegisterPubSubProxyServer(gs, ps)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxGRPCMessageBytes), grpc.MaxCallSendMsgSize(maxGRPCMessageBytes)))
	noErr(t, err, "dial proxy")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		ps.initiateShutdown(500 * time.Millisecond)
		ps.waitClosed()
		gs.Stop()
		_ = tm.Shutdown(context.Background())
	})
	return &testEnv{t, ctx, proxypb.NewPubSubProxyClient(conn), ps, tm}
}

type fakePubSubCall struct {
	method  string
	md      metadata.MD
	pubReq  *pubsubpb.PublishRequest
	ackReq  *pubsubpb.AcknowledgeRequest
	modReq  *pubsubpb.ModifyAckDeadlineRequest
	pullReq *pubsubpb.StreamingPullRequest
}
type fakeHandlers struct {
	pubHandler  func(context.Context, *pubsubpb.PublishRequest, metadata.MD) (*pubsubpb.PublishResponse, error)
	ackHandler  func(context.Context, *pubsubpb.AcknowledgeRequest, metadata.MD) (*emptypb.Empty, error)
	modHandler  func(context.Context, *pubsubpb.ModifyAckDeadlineRequest, metadata.MD) (*emptypb.Empty, error)
	pullHandler func(pubsubpb.Subscriber_StreamingPullServer, int32) error
}
type fakePubSubServer struct {
	pubsubpb.UnimplementedPublisherServer
	pubsubpb.UnimplementedSubscriberServer
	fakeHandlers
	mu        sync.Mutex
	calls     []fakePubSubCall
	callCh    chan struct{}
	streamCh  chan *pubsubpb.StreamingPullResponse
	pull1Err  chan error
	nextID    atomic.Int64
	streamSeq atomic.Int32
}

func startFakePubSubServer(t *testing.T) (*fakePubSubServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	noErr(t, err, "fake listen")
	fs, gs := &fakePubSubServer{callCh: make(chan struct{}, 256), streamCh: make(chan *pubsubpb.StreamingPullResponse, 64), pull1Err: make(chan error, 1)}, grpc.NewServer()
	pubsubpb.RegisterPublisherServer(gs, fs)
	pubsubpb.RegisterSubscriberServer(gs, fs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return fs, lis.Addr().String()
}
func (f *fakePubSubServer) snapshotCalls() []fakePubSubCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}
func (f *fakePubSubServer) waitCall(t *testing.T, pred func([]fakePubSubCall) bool) []fakePubSubCall {
	t.Helper()
	timer := time.NewTimer(25 * time.Second)
	defer timer.Stop()
	for {
		if calls := f.snapshotCalls(); pred(calls) {
			return calls
		}
		select {
		case <-f.callCh:
		case <-timer.C:
			t.Fatalf("timed out waiting for fake RPC (saw %d)", len(f.snapshotCalls()))
		}
	}
}
func (f *fakePubSubServer) notify() {
	select {
	case f.callCh <- struct{}{}:
	default:
	}
}
func (f *fakePubSubServer) rec(ctx context.Context, c fakePubSubCall) metadata.MD {
	md, _ := metadata.FromIncomingContext(ctx)
	c.md = md.Copy()
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
	f.notify()
	return md
}
func fakeRPC[Req, Resp any](f *fakePubSubServer, ctx context.Context, c fakePubSubCall, fn func(context.Context, Req, metadata.MD) (Resp, error), r Req, def func() Resp) (Resp, error) {
	md := f.rec(ctx, c)
	if fn != nil {
		defer f.notify()
		return fn(ctx, r, md)
	}
	return def(), nil
}
func (f *fakePubSubServer) Publish(ctx context.Context, r *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	return fakeRPC(f, ctx, fakePubSubCall{method: "Publish", pubReq: proto.CloneOf(r)}, f.pubHandler, r, func() *pubsubpb.PublishResponse {
		ids := make([]string, len(r.GetMessages()))
		for i := range ids {
			ids[i] = fmt.Sprintf("msg-%d", f.nextID.Add(1))
		}
		return &pubsubpb.PublishResponse{MessageIds: ids}
	})
}
func (f *fakePubSubServer) Acknowledge(ctx context.Context, r *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	return fakeRPC(f, ctx, fakePubSubCall{method: "Acknowledge", ackReq: proto.CloneOf(r)}, f.ackHandler, r, func() *emptypb.Empty { return &emptypb.Empty{} })
}
func (f *fakePubSubServer) ModifyAckDeadline(ctx context.Context, r *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	return fakeRPC(f, ctx, fakePubSubCall{method: "ModifyAckDeadline", modReq: proto.CloneOf(r)}, f.modHandler, r, func() *emptypb.Empty { return &emptypb.Empty{} })
}
func (f *fakePubSubServer) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	seq := f.streamSeq.Add(1)
	if f.pullHandler != nil {
		r0, err := stream.Recv()
		if err != nil {
			return err
		}
		f.rec(stream.Context(), fakePubSubCall{method: "StreamingPull", pullReq: proto.CloneOf(r0)})
		if herr := f.pullHandler(stream, seq); herr != nil {
			return herr
		}
	}
	doneRecv := make(chan struct{})
	go func() {
		defer close(doneRecv)
		for rq, err := stream.Recv(); err == nil; rq, err = stream.Recv() {
			f.rec(stream.Context(), fakePubSubCall{method: "StreamingPull", pullReq: proto.CloneOf(rq)})
		}
	}()
	killCh := map[bool]<-chan error{true: f.pull1Err}[seq == 1]
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-doneRecv:
			return nil
		case err := <-killCh:
			return err
		case resp, ok := <-f.streamCh:
			if !ok || stream.Send(resp) != nil {
				return nil
			}
		}
	}
}
func mkRM(ackID, msgID, data string, attempt int32, pt *timestamppb.Timestamp, ordKey string, attrs map[string]string) *pubsubpb.ReceivedMessage {
	pt = cmp.Or(pt, timestamppb.Now())
	return &pubsubpb.ReceivedMessage{AckId: ackID, DeliveryAttempt: attempt, Message: &pubsubpb.PubsubMessage{MessageId: msgID, Data: []byte(data), Attributes: attrs, OrderingKey: ordKey, PublishTime: pt}}
}
func rmPT(a, m, d, k string) *pubsubpb.ReceivedMessage { return mkRM(a, m, d, 1, sharedPT, k, nil) }
func modReq(dl int32, ids ...string) *pubsubpb.ModifyAckDeadlineRequest {
	return &pubsubpb.ModifyAckDeadlineRequest{AckIds: ids, AckDeadlineSeconds: dl}
}
func pullResp(eod bool, rms ...*pubsubpb.ReceivedMessage) *pubsubpb.StreamingPullResponse {
	ord := slices.ContainsFunc(rms, func(r *pubsubpb.ReceivedMessage) bool { return r.GetMessage().GetOrderingKey() != "" })
	return &pubsubpb.StreamingPullResponse{SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: eod, MessageOrderingEnabled: ord}, ReceivedMessages: rms}
}
func pushFakeBatch(f *fakePubSubServer, eod bool, rms ...*pubsubpb.ReceivedMessage) {
	f.streamCh <- pullResp(eod, rms...)
}
func pushFakeMsg(f *fakePubSubServer, ackID, msgID, data string, eod bool, attempt int32) {
	pushFakeBatch(f, eod, mkRM(ackID, msgID, data, attempt, nil, "", nil))
}
func eiInvalidArg(ackID, metaVal string) error {
	st, _ := status.New(codes.InvalidArgument, "eod").WithDetails(&errdetails.ErrorInfo{Reason: "EXACTLY_ONCE_ACKID_FAILURE", Metadata: map[string]string{ackID: metaVal}})
	return st.Err()
}
func waitModAck(t *testing.T, f *fakePubSubServer, wantDL int32, ackIDs ...string) {
	t.Helper()
	f.waitCall(t, func(cs []fakePubSubCall) bool {
		return !slices.ContainsFunc(ackIDs, func(id string) bool {
			return !slices.ContainsFunc(cs, func(c fakePubSubCall) bool {
				return c.method == "ModifyAckDeadline" && (wantDL < 0 || c.modReq.GetAckDeadlineSeconds() == wantDL) && slices.Contains(c.modReq.GetAckIds(), id)
			})
		})
	})
}
func once(ch chan struct{}) func() { return sync.OnceFunc(func() { close(ch) }) }
func waitCh[T any](ctx context.Context, ch <-chan T) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func req(t *testing.T, ok bool, m string, a ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(m, a...)
	}
}
func noErr(t *testing.T, err error, msg string) { t.Helper(); req(t, err == nil, "%s: %v", msg, err) }
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	req(t, status.Code(err) == want, "got code %v (%v), want %v", status.Code(err), err, want)
}
func (e *testEnv) createPub(r *proxypb.CreatePublisherRequest) string {
	e.t.Helper()
	resp, err := e.client.CreatePublisher(e.ctx, r)
	noErr(e.t, err, "CreatePublisher")
	return resp.GetPublisherId()
}
func (e *testEnv) pub(r *proxypb.PublishRequest) *proxypb.PublishResponse {
	e.t.Helper()
	resp, err := e.client.Publish(e.ctx, r)
	noErr(e.t, err, "Publish")
	return resp
}
func (e *testEnv) pubStr(id, s string) *proxypb.PublishResponse {
	e.t.Helper()
	return e.pub(&proxypb.PublishRequest{PublisherId: id, Data: []byte(s)})
}
func (e *testEnv) pubOnce(r *proxypb.CreatePublisherRequest, data string) *proxypb.PublishResponse {
	e.t.Helper()
	id := e.createPub(r)
	defer func() { _, _ = e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: id}) }()
	return e.pubStr(id, data)
}
func (e *testEnv) subErr(cmd *proxypb.SubscriberCommand) (*proxypb.SubscriberEvent, error) {
	ss, err := e.client.SubscriberSession(e.ctx)
	noErr(e.t, err, "SubscriberSession")
	if cmd != nil {
		_ = ss.Send(cmd)
	} else {
		_ = ss.CloseSend()
	}
	return ss.Recv()
}

type subSess struct {
	t *testing.T
	proxypb.PubSubProxy_SubscriberSessionClient
}

func (e *testEnv) startSubCtx(ctx context.Context, start *proxypb.StartSubscriberCommand) *subSess {
	e.t.Helper()
	sess, err := e.client.SubscriberSession(ctx)
	noErr(e.t, err, "SubscriberSession")
	noErr(e.t, sess.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: start}}), "send start")
	ev, err := sess.Recv()
	req(e.t, err == nil && ev.GetStarted() != nil, "expected StartedEvent, got ev=%v err=%v", ev, err)
	return &subSess{e.t, sess}
}
func (e *testEnv) openFakeSub(ctx context.Context, c *proxypb.StartSubscriberCommand, h ...fakeHandlers) (*fakePubSubServer, *subSess) {
	e.t.Helper()
	f, addr := startFakePubSubServer(e.t)
	c.Endpoint, c.Subscription = addr, "projects/p/subscriptions/"+c.GetSubscription()
	if len(h) > 0 {
		f.fakeHandlers = h[0]
	}
	return f, e.startSubCtx(cmp.Or(ctx, e.ctx), c)
}
func (s *subSess) md() *proxypb.MessageDeliveredEvent {
	s.t.Helper()
	ev, err := s.Recv()
	req(s.t, err == nil && ev.GetMessageDelivered() != nil, "want MD, got %v err=%v", ev, err)
	return ev.GetMessageDelivered()
}
func (s *subSess) sr() *proxypb.SettlementResultEvent {
	s.t.Helper()
	ev, err := s.Recv()
	req(s.t, err == nil && ev.GetSettlementResult() != nil, "want SR, got %v err=%v", ev, err)
	return ev.GetSettlementResult()
}
func (s *subSess) stopCount() (*proxypb.StoppedEvent, int64) {
	s.t.Helper()
	noErr(s.t, s.Send(stopSubCmd), "send stop")
	for md := int64(0); ; {
		ev, err := s.Recv()
		noErr(s.t, err, "recv StoppedEvent")
		if ev.GetMessageDelivered() != nil {
			md++
		} else if st := ev.GetStopped(); st != nil {
			return st, md
		}
	}
}
func (s *subSess) stop() *proxypb.StoppedEvent { s.t.Helper(); st, _ := s.stopCount(); return st }
func (s *subSess) settle(tok string, act proxypb.SettleMessageCommand_SettleAction) {
	s.t.Helper()
	noErr(s.t, s.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: act}}}), "settle")
}
func (s *subSess) ack(md *proxypb.MessageDeliveredEvent) {
	s.t.Helper()
	s.settle(md.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK)
}
func (s *subSess) ackNext() *proxypb.MessageDeliveredEvent { md := s.md(); s.ack(md); return md }

func TestGetCapabilitiesAndDefaults(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	caps, err := e.client.GetCapabilities(e.ctx, &proxypb.GetCapabilitiesRequest{})
	noErr(t, err, "GetCapabilities")
	req(t, caps.GetClientLanguage() == "go" && caps.GetClientLibrary() == "cloud.google.com/go/pubsub/v2" && caps.GetClientVersion() == "2.9.0" && caps.GetSupportsMonitoring() && !caps.GetSupportsUnaryPull() && !caps.GetSupportsBlockingPublish() && !caps.GetSupportsAutoNackOnDrop() && caps.GetSupportsPublishFlowControl() && caps.GetSupportsPublishCompression() && caps.GetSupportsPublishHedging() && caps.GetSupportsOrderingOptIn() && caps.GetSupportsSubscriberOrdering() && caps.GetSupportsKeepalive() && caps.GetSupportsEodReceiptModack() && caps.GetSupportsAckWithResult() && caps.GetSupportsCustomRetryCodes() && caps.GetSupportsExactlyOnceDelivery() && caps.GetSupportsPerStreamFlowControl() && caps.GetSupportsResumePublish() && caps.GetSupportsShutdownBehavior() && caps.GetSupportsShutdownTimeout(), "unexpected caps: %+v", caps)
	d := caps.GetDefaults()
	req(t, d != nil && d.GetPublishBatchCountThreshold() == 100 && d.GetPublishBatchByteThreshold() == 1_000_000 && d.GetPublishMaxRequestBytes() == 10_000_000 && d.GetPublishBatchDelayMs() == 10 && d.GetPublishTotalTimeoutMs() == 60_000 && d.GetPublishFlowControlMaxMessages() == 1000 && d.GetPublishFlowControlMaxBytes() == -1 && d.GetPublishFlowControlBehavior() == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE && !d.GetPublishCompressionEnabledByDefault() && d.GetPublishCompressionBytesThreshold() == 240 && d.GetPublishCompressionAlgorithm() == proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP && d.GetPublishHedgingDelayMs() == 1000 && d.GetPublishHedgingMaxTokens() == 50 && d.GetPublishHedgingRefillRatio() == 0.1 && d.GetSubscriberMaxOutstandingMessages() == 1000 && d.GetSubscriberMaxOutstandingBytes() == 1_000_000_000 && d.GetSubscriberMaxExtensionMs() == 3_600_000 && d.GetSubscriberMinAckExtensionMs() == 10_000 && d.GetSubscriberMaxAckExtensionMs() == 600_000 && d.GetSubscriberAdaptiveAckExtension() && !d.GetSubscriberPerStreamFlowControl() && d.GetSubscriberDefaultStreamAckDeadlineSeconds() == 60 && d.GetKeepalivePingIntervalMs() == 30_000 && d.GetKeepalivePongTimeoutMs() == 15_000 && d.GetRetryInitialDelayMs() == 100 && d.GetRetryMaxDelayMs() == 60_000 && d.GetRetryDelayMultiplier() == 4.0, "unexpected defaults: %+v", d)
}

func TestUnimplementedAndValidationErrors(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	fakeSrv, fakeAddr := startFakePubSubServer(t)
	pubID, chk := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t", CountThreshold: proto.Int64(1)}), func(want codes.Code) func(any, error) {
		return func(_ any, err error) { t.Helper(); assertCode(t, err, want) }
	}
	chk(codes.Unimplemented)(e.client.UnaryPull(e.ctx, &proxypb.UnaryPullRequest{}))
	chk(codes.Unimplemented)(e.client.CreatePublisher(e.ctx, &proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t", CompressionAlgorithm: proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE}))
	chk(codes.InvalidArgument)(e.client.CreatePublisher(e.ctx, &proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t", NumWorkers: proto.Int32(-1)}))
	chk(codes.Unimplemented)(e.client.Publish(e.ctx, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("hi"), UseBlockingPublisher: true}))
	chk(codes.NotFound)(e.client.AwaitPublish(e.ctx, &proxypb.AwaitPublishRequest{PublisherId: "missing", PublishToken: "t"}))
	chk(codes.NotFound)(e.client.AwaitPublish(e.ctx, &proxypb.AwaitPublishRequest{PublisherId: pubID, PublishToken: "bad"}))
	chk(codes.NotFound)(e.client.FlushPublisher(e.ctx, &proxypb.FlushPublisherRequest{PublisherId: "missing"}))
	chk(codes.NotFound)(e.client.StopPublisher(e.ctx, &proxypb.StopPublisherRequest{PublisherId: "missing"}))
	chk(codes.NotFound)(e.client.ResumePublish(e.ctx, &proxypb.ResumePublishRequest{PublisherId: "missing"}))
	chk(codes.NotFound)(e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: "missing"}))
	for _, tc := range []struct {
		init, max *int64
		mult      *float64
	}{{init: proto.Int64(-10)}, {max: proto.Int64(-5)}, {mult: proto.Float64(-0.5)}, {mult: proto.Float64(math.NaN())}, {mult: proto.Float64(math.Inf(1))}, {mult: proto.Float64(math.Inf(-1))}} {
		chk(codes.InvalidArgument)(e.client.CreatePublisher(e.ctx, &proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t", InitialRetryDelayMs: tc.init, MaxRetryDelayMs: tc.max, RetryDelayMultiplier: tc.mult}))
		chk(codes.InvalidArgument)(e.subErr(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: &proxypb.StartSubscriberCommand{Endpoint: fakeAddr, Subscription: "projects/p/subscriptions/s", InitialRetryDelayMs: tc.init, MaxRetryDelayMs: tc.max, RetryDelayMultiplier: tc.mult}}}))
	}
	req(t, len(fakeSrv.snapshotCalls()) == 0, "validation leaked wire calls")
	chk(codes.InvalidArgument)(e.subErr(nil))
	chk(codes.InvalidArgument)(e.subErr(stopSubCmd))
	e.srv.shuttingDown.Store(true)
	chk(codes.Unavailable)(e.client.CreatePublisher(e.ctx, &proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t"}))
	chk(codes.Unavailable)(e.subErr(nil))
	e.srv.shuttingDown.Store(false)
}

func TestTracingFullCoverageAndLimits(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	_, _ = e.client.ResetTraces(e.ctx, &proxypb.ResetTracesRequest{})
	tr, links := otel.Tracer("test.tracer", otrace.WithInstrumentationVersion("1.2.3")), make([]otrace.Link, 200)
	for i := range links {
		links[i] = otrace.Link{SpanContext: otrace.NewSpanContext(otrace.SpanContextConfig{TraceID: otrace.TraceID{15: byte(i + 1)}, SpanID: otrace.SpanID{7: byte(i + 1)}, TraceFlags: otrace.FlagsSampled}), Attributes: []attribute.KeyValue{attribute.Int("idx", i)}}
	}
	sc := otrace.SpanContextFromContext(extractRemoteTraceContext(e.ctx, "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01", "vendor=v1"))
	tp, ts := injectCallbackTraceContext(e.ctx)
	req(t, sc.IsValid() && sc.TraceState().Get("vendor") == "v1" && tp == "" && ts == "", "trace ctx")
	rootCtx, root := tr.Start(e.ctx, "root-ok", otrace.WithSpanKind(otrace.SpanKindProducer), otrace.WithLinks(links...), otrace.WithAttributes(attribute.String("s", "v"), attribute.Int64("i", 42), attribute.Float64("f", 3.5), attribute.Bool("b", true), attribute.StringSlice("ss", []string{"a", "b"}), attribute.Int64Slice("is", []int64{1, 2}), attribute.Float64Slice("fs", []float64{1.25}), attribute.BoolSlice("bs", []bool{true, false})))
	root.AddEvent("ev1", otrace.WithAttributes(attribute.String("ek", "ev")))
	root.SetStatus(otelcodes.Ok, "all-good")
	_, child := tr.Start(rootCtx, "child-err", otrace.WithSpanKind(otrace.SpanKindConsumer))
	child.SetStatus(otelcodes.Error, "boom")
	child.End()
	root.End()
	drain, err := e.client.DrainTraces(e.ctx, &proxypb.DrainTracesRequest{})
	req(t, err == nil && len(drain.GetSpans()) == 2, "DrainTraces %d %v", len(drain.GetSpans()), err)
	rs, cs := drain.GetSpans()[0], drain.GetSpans()[1]
	if rs.GetName() != "root-ok" {
		rs, cs = cs, rs
	}
	req(t, rs != nil && rs.GetParentSpanId() == "" && rs.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK && rs.GetKind() == proxypb.SpanKind_SPAN_KIND_PRODUCER && len(rs.GetLinks()) == 200 && rs.GetDroppedLinksCount() == 0 && len(rs.GetAttributes()) == 8 && cs != nil && cs.GetParentSpanId() == rs.GetSpanId() && cs.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR && cs.GetStatusMessage() == "boom", "spans")
	_, _ = e.client.ResetTraces(e.ctx, &proxypb.ResetTracesRequest{})
	d2, err := e.client.DrainTraces(e.ctx, &proxypb.DrainTracesRequest{})
	req(t, err == nil && len(d2.GetSpans()) == 0, "reset")
	for in, want := range map[otrace.SpanKind]proxypb.SpanKind{otrace.SpanKindInternal: proxypb.SpanKind_SPAN_KIND_INTERNAL, otrace.SpanKindServer: proxypb.SpanKind_SPAN_KIND_SERVER, otrace.SpanKindClient: proxypb.SpanKind_SPAN_KIND_CLIENT, otrace.SpanKindProducer: proxypb.SpanKind_SPAN_KIND_PRODUCER, otrace.SpanKindConsumer: proxypb.SpanKind_SPAN_KIND_CONSUMER, otrace.SpanKindUnspecified: proxypb.SpanKind_SPAN_KIND_UNSPECIFIED} {
		req(t, convertSpanKind(in) == want, "kind(%v)", in)
	}
	var nilTM *traceManager
	nilTM.Reset(e.ctx)
	req(t, nilTM.Drain(e.ctx) == nil && nilTM.Shutdown(e.ctx) == nil && e.tm.store.ExportSpans(e.ctx, nil) == nil && convertAttributes([]attribute.KeyValue{{Key: "inv"}}) == nil, "nil/empty")
}

func TestConfigProjectIDHeadersAndEmulatorIsolation(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	fakeSrv, fakeAddr := startFakePubSubServer(t)
	_, err := e.client.CreatePublisher(e.ctx, &proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "short-topic"})
	assertCode(t, err, codes.InvalidArgument)
	ev, err := e.subErr(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: &proxypb.StartSubscriberCommand{Endpoint: fakeAddr, Subscription: "short-sub"}}})
	req(t, err == nil && ev.GetStopped().GetErrorCode() == int32(codes.InvalidArgument) && len(fakeSrv.snapshotCalls()) == 0, "short sub %v %v", ev, err)
	const bogusEmu = "127.0.0.1:1"
	t.Setenv("PUBSUB_EMULATOR_HOST", bogusEmu)
	pubID := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "my-topic", ProjectId: "explicit-p", CountThreshold: proto.Int64(1), CustomHeaders: map[string]string{"x-test-hdr": "hdr-val"}})
	req(t, os.Getenv("PUBSUB_EMULATOR_HOST") == bogusEmu && e.pubStr(pubID, "hello").GetErrorCode() == 0, "emu/publish")
	_, _ = e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: pubID})
	calls := fakeSrv.snapshotCalls()
	req(t, len(calls) == 1 && calls[0].pubReq.GetTopic() == "projects/explicit-p/topics/my-topic" && slices.Equal(calls[0].md.Get("x-test-hdr"), []string{"hdr-val"}), "calls: %+v", calls)
	fHdr, sess := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "hdr-sub", CustomHeaders: map[string]string{"x-sub-hdr": "sval"}}, fakeHandlers{pullHandler: func(_ pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
		return map[bool]error{true: status.Error(codes.Unavailable, "force reconnect")}[seq == 1]
	}})
	pushFakeMsg(fHdr, "ack-hdr", "m-hdr", "ok", false, 0)
	sess.ack(sess.md())
	_ = sess.stop()
	pullCalls := slices.DeleteFunc(fHdr.snapshotCalls(), func(c fakePubSubCall) bool { return c.method != "StreamingPull" })
	req(t, len(pullCalls) >= 2 && !slices.ContainsFunc(pullCalls, func(c fakePubSubCall) bool { return !slices.Equal(c.md.Get("x-sub-hdr"), []string{"sval"}) }), "StreamingPull=%d", len(pullCalls))
	_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
	_, _ = e.client.ResetTraces(e.ctx, &proxypb.ResetTracesRequest{})
	emuID := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/emu-p/topics/emu-t", CountThreshold: proto.Int64(1), UseEmulatorEnv: true, EnableMonitoring: true})
	_, set := os.LookupEnv("PUBSUB_EMULATOR_HOST")
	req(t, !set && e.pubStr(emuID, "emu").GetErrorCode() == 0, "emulator")
	_, _ = e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: emuID})
	spans, err := e.client.DrainTraces(e.ctx, &proxypb.DrainTracesRequest{})
	req(t, err == nil && len(spans.GetSpans()) > 0, "spans")
}

func TestPublisherSettingsRetryTimeoutResumeAndImmutability(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	fakeSrv, fakeAddr := startFakePubSubServer(t)
	var seenLongDL, seenShortDL, dlAttempts atomic.Int64
	pubErrs := map[string]codes.Code{"dl": codes.DataLoss, "unavail": codes.Unavailable, "ord-fail": codes.InvalidArgument}
	fakeSrv.pubHandler = func(pctx context.Context, rq *pubsubpb.PublishRequest, _ metadata.MD) (*pubsubpb.PublishResponse, error) {
		dl, _ := pctx.Deadline()
		if d := string(rq.GetMessages()[0].GetData()); d == "long" {
			seenLongDL.Store(time.Until(dl).Milliseconds())
		} else if d == "short" {
			seenShortDL.Store(time.Until(dl).Milliseconds())
		} else if c, ok := pubErrs[d]; ok {
			if d == "dl" {
				dlAttempts.Add(1)
			}
			return nil, status.Error(c, d)
		}
		return &pubsubpb.PublishResponse{MessageIds: []string{"ok-1"}}, nil
	}
	req(t, e.pubOnce(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t-long", CountThreshold: proto.Int64(1), TotalTimeoutMs: proto.Int64(90_000)}, "long").GetErrorCode() == 0 && seenLongDL.Load() > 65_000, "long dl=%dms", seenLongDL.Load())
	pOrd := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t-ord", EnableMessageOrdering: proto.Bool(true), CountThreshold: proto.Int64(1), ByteThreshold: proto.Int64(4096), DelayThresholdMs: proto.Int64(5), NumWorkers: proto.Int32(2), MaxOutstandingMessages: proto.Int64(10), MaxOutstandingBytes: proto.Int64(65536), LimitExceededBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE, EnableCompression: proto.Bool(true), CompressionBytesThreshold: proto.Int64(10)})
	req(t, e.pub(&proxypb.PublishRequest{PublisherId: pOrd, OrderingKey: "k1", Data: []byte("ord-fail")}).GetErrorCode() != 0, "ord-fail")
	req(t, e.pub(&proxypb.PublishRequest{PublisherId: pOrd, OrderingKey: "k1", Data: []byte("paused")}).GetErrorCode() != 0, "paused")
	_, err := e.client.ResumePublish(e.ctx, &proxypb.ResumePublishRequest{PublisherId: pOrd, OrderingKey: "k1"})
	noErr(t, err, "ResumePublish")
	req(t, e.pub(&proxypb.PublishRequest{PublisherId: pOrd, OrderingKey: "k1", Data: []byte("resumed-over-10b")}).GetErrorCode() == 0, "resumed")
	_, _ = e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: pOrd})
	pubID := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t-ret", EnableMonitoring: true, CountThreshold: proto.Int64(1), TotalTimeoutMs: proto.Int64(250), LimitExceededBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK, Hedging: &proxypb.HedgingSettings{Enabled: true, DelayMs: proto.Int64(200), MaxTokens: proto.Int64(5), RefillRatio: proto.Float64(0.1)}, RetryCodes: []int32{int32(codes.DataLoss)}, InitialRetryDelayMs: proto.Int64(5), MaxRetryDelayMs: proto.Int64(10), RetryDelayMultiplier: proto.Float64(0.5), MaxAttempts: proto.Int32(2)})
	req(t, e.pub(&proxypb.PublishRequest{PublisherId: pubID, Data: []byte("short"), VerifyAttributesImmutability: true}).GetErrorCode() == 0 && seenShortDL.Load() <= 400, "short dl=%d", seenShortDL.Load())
	req(t, e.pubStr(pubID, "dl").GetErrorCode() == int32(codes.DataLoss) && dlAttempts.Load() == 2, "DataLoss %d", dlAttempts.Load())
	req(t, e.pubStr(pubID, "unavail").GetErrorCode() == int32(codes.Unavailable), "Unavailable")
	beforeCalls := len(fakeSrv.snapshotCalls())
	for _, ra := range []*proxypb.RawAttribute{{Key: []byte("a\xc5z"), Value: []byte("v")}, {Key: []byte("k"), Value: []byte("v\xc5z")}} {
		req(t, e.pub(&proxypb.PublishRequest{PublisherId: pubID, Data: []byte("bad"), RawAttributes: []*proxypb.RawAttribute{ra}}).GetErrorCode() == int32(codes.Internal) && len(fakeSrv.snapshotCalls()) == beforeCalls, "UTF-8")
	}
	asyncResp := e.pub(&proxypb.PublishRequest{PublisherId: pubID, Data: []byte("ok"), Attributes: map[string]string{"k": "v"}, ParentTraceparent: "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01", AsyncFire: true, VerifyAttributesImmutability: true})
	awaitResp, err := e.client.AwaitPublish(e.ctx, &proxypb.AwaitPublishRequest{PublisherId: pubID, PublishToken: asyncResp.GetPublishToken()})
	req(t, asyncResp.GetPublishToken() != "" && !asyncResp.GetCallerAttributesMutated() && err == nil && awaitResp.GetErrorCode() == 0 && awaitResp.GetMessageId() == "ok-1" && !awaitResp.GetCallerAttributesMutated(), "async/Await")
	_, err = e.client.AwaitPublish(e.ctx, &proxypb.AwaitPublishRequest{PublisherId: pubID, PublishToken: asyncResp.GetPublishToken()})
	assertCode(t, err, codes.NotFound)
	_, _ = e.client.FlushPublisher(e.ctx, &proxypb.FlushPublisherRequest{PublisherId: pubID})
	_, _ = e.client.StopPublisher(e.ctx, &proxypb.StopPublisherRequest{PublisherId: pubID})
	req(t, e.pubStr(pubID, "after").GetErrorCode() != 0, "post-stop")
	_, _ = e.client.ClosePublisher(e.ctx, &proxypb.ClosePublisherRequest{PublisherId: pubID})
	_, err = e.client.Publish(e.ctx, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("gone")})
	assertCode(t, err, codes.NotFound)
	req(t, e.pubOnce(&proxypb.CreatePublisherRequest{Endpoint: fakeAddr, Topic: "projects/p/topics/t-sig", CountThreshold: proto.Int64(1), MaxOutstandingMessages: proto.Int64(1), MaxOutstandingBytes: proto.Int64(1), LimitExceededBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR, MaxAttempts: proto.Int32(0)}, "too-big").GetErrorCode() != 0, "SignalError")
}

func TestSubscriberLifecycleNackImmediatelyAndAbortCleanup(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	fake1, s1 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "s1", AutoSettleMode: interactive, EnableMonitoring: true, ShutdownBehavior: waitBhv}, fakeHandlers{ackHandler: func(_ context.Context, r *pubsubpb.AcknowledgeRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if slices.Contains(r.GetAckIds(), "ack-2") {
			return nil, eiInvalidArg("ack-2", "PERMANENT_FAILURE_INVALID_ACK_ID")
		}
		return &emptypb.Empty{}, nil
	}})
	pushFakeBatch(fake1, true, mkRM("ack-2", "m2", "two", 3, nil, "", nil))
	md2 := s1.md()
	req(t, md2.GetIsExactlyOnce() && md2.GetDeliveryAttempt() == 3 && md2.GetCallbackTraceparent() != "", "delivery: %+v", md2)
	noErr(t, s1.Send(stopSubCmd), "stop")
	evCh := make(chan *proxypb.SubscriberEvent, 4)
	go func() {
		defer close(evCh)
		for ev, err := s1.Recv(); err == nil; ev, err = s1.Recv() {
			evCh <- ev
		}
	}()
	select {
	case u := <-evCh:
		t.Fatalf("stopped prematurely: %v", u)
	case <-time.After(80 * time.Millisecond):
	}
	s1.settle(md2.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT)
	gotSR, gotStop := (<-evCh).GetSettlementResult(), (<-evCh).GetStopped()
	req(t, gotSR.GetStatus() == proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID && gotSR.GetGrpcErrorCode() == int32(codes.InvalidArgument) && gotStop.GetTotalDelivered() == 1, "sr=%+v stop=%+v", gotSR, gotStop)
	for _, tmMs := range []int64{10_000, -1, math.MaxInt64} {
		f, sess := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "nack-imm", AutoSettleMode: interactive, ShutdownBehavior: nackBhv, ShutdownTimeoutMs: proto.Int64(tmMs)})
		pushFakeMsg(f, "ack-ni", "msg-ni", "ni", false, 0)
		_ = sess.md()
		t0 := time.Now()
		req(t, sess.stop().GetTotalDelivered() == 1 && time.Since(t0) <= time.Second, "NACK_IMMEDIATELY(%dms)", tmMs)
		waitModAck(t, f, 0, "ack-ni")
	}
	streamDead := make(chan struct{})
	zCtx, zCancel := context.WithCancel(e.ctx)
	fZ, zSess := e.openFakeSub(zCtx, &proxypb.StartSubscriberCommand{Subscription: "zombie-1", AutoSettleMode: interactive, ShutdownBehavior: waitBhv, ShutdownTimeoutMs: proto.Int64(-1)}, fakeHandlers{pullHandler: func(st pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
		if seq == 1 {
			_ = st.Send(pullResp(false, mkRM("ack-z", "m-z", "z", 0, nil, "", nil)))
			<-streamDead
			return status.Error(codes.Unavailable, "reset")
		}
		return nil
	}})
	_ = zSess.md()
	_ = zSess.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: nackBhv}}})
	_ = zSess.CloseSend()
	zCancel()
	close(streamDead)
	time.Sleep(150 * time.Millisecond)
	req(t, !slices.ContainsFunc(fZ.snapshotCalls(), func(c fakePubSubCall) bool {
		return c.method == "ModifyAckDeadline" && c.modReq.GetAckDeadlineSeconds() == 0
	}) && fZ.streamSeq.Load() == 1, "zombie streams=%d", fZ.streamSeq.Load())
	t0 := time.Now()
	e.srv.initiateShutdown(0)
	e.srv.waitClosed()
	req(t, time.Since(t0) <= time.Second, "shutdown %v", time.Since(t0))
}

func TestSubscriberSettingsAutoModesRetryAndAckStatus(t *testing.T) {
	e := newTestEnv(t, 20*time.Second)
	for _, mode := range []proxypb.StartSubscriberCommand_AutoSettleMode{autoAck, autoNack, autoDrop, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT} {
		f, sess := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "s", AutoSettleMode: mode, ShutdownBehavior: waitBhv, ShutdownTimeoutMs: proto.Int64(250)})
		pushFakeMsg(f, "ack-auto", "msg-auto", "p", mode == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT, 0)
		req(t, sess.md().GetAutoSettled(), "AutoSettled(%v)", mode)
		if mode == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT {
			req(t, sess.sr().GetIsNack(), "IsNack")
		} else if mode != autoDrop {
			f.waitCall(t, func(cs []fakePubSubCall) bool {
				return slices.ContainsFunc(cs, func(c fakePubSubCall) bool {
					return c.method == "Acknowledge" || (c.method == "ModifyAckDeadline" && c.modReq.GetAckDeadlineSeconds() == 0)
				})
			})
		}
		st := sess.stop()
		req(t, st.GetErrorCode() == 0 && st.GetTotalDelivered() == 1, "Stopped(%v)=%+v", mode, st)
	}
	ackErrs := map[string]error{"a-pd": status.Error(codes.PermissionDenied, "pd"), "a-fp": status.Error(codes.FailedPrecondition, "fp"), "a-oth": eiInvalidArg("a-oth", "PERMANENT_FAILURE_OTHER"), "a-oor": status.Error(codes.OutOfRange, "oor")}
	var ackAttempt atomic.Int32
	f2, sess2 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "sub-opts", AutoSettleMode: interactive, MaxOutstandingMessages: proto.Int64(5), MaxOutstandingBytes: proto.Int64(1 << 20), EnablePerStreamFlowControl: proto.Bool(true), NumStreams: proto.Int32(1), MaxExtensionMs: proto.Int64(0), MinDurationPerAckExtensionMs: proto.Int64(10_000), MaxDurationPerAckExtensionMs: proto.Int64(20_000), ShutdownBehavior: waitBhv, ShutdownTimeoutMs: proto.Int64(120), RetryCodes: []int32{int32(codes.DataLoss)}, InitialRetryDelayMs: proto.Int64(5), MaxRetryDelayMs: proto.Int64(10), RetryDelayMultiplier: proto.Float64(1.0)}, fakeHandlers{ackHandler: func(_ context.Context, r *pubsubpb.AcknowledgeRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if len(r.GetAckIds()) == 1 && ackErrs[r.GetAckIds()[0]] != nil {
			return nil, ackErrs[r.GetAckIds()[0]]
		} else if ackAttempt.Add(1) == 1 {
			return nil, status.Error(codes.DataLoss, "retry-ack")
		}
		return &emptypb.Empty{}, nil
	}})
	pushFakeMsg(f2, "a-ack", "m-ack", "0", false, 0)
	sess2.ack(sess2.md())
	f2.waitCall(t, func(_ []fakePubSubCall) bool { return ackAttempt.Load() >= 2 })
	pushFakeMsg(f2, "a-n0", "m-n0", "n", false, 0)
	sess2.settle(sess2.md().GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK)
	waitModAck(t, f2, 0, "a-n0")
	pushFakeMsg(f2, "a-n1", "m-n1", "1", true, 0)
	sess2.settle(sess2.md().GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT)
	req(t, sess2.sr().GetIsNack(), "IsNack")
	for _, tc := range []struct {
		id   string
		want proxypb.SettlementResultEvent_AckStatus
		code codes.Code
	}{{"a-pd", proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, codes.PermissionDenied}, {"a-fp", proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, codes.FailedPrecondition}, {"a-oth", proxypb.SettlementResultEvent_ACK_STATUS_OTHER, codes.Unknown}, {"a-oor", proxypb.SettlementResultEvent_ACK_STATUS_OTHER, codes.OutOfRange}} {
		pushFakeMsg(f2, tc.id, "m-"+tc.id, "s", true, 0)
		sess2.settle(sess2.md().GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT)
		sr := sess2.sr()
		req(t, sr.GetStatus() == tc.want && sr.GetGrpcErrorCode() == int32(tc.code), "settle %s: %+v", tc.id, sr)
	}
	pushFakeMsg(f2, "a-rws", "m-rws", "2", false, 0)
	_ = sess2.Send(&proxypb.SubscriberCommand{})
	sess2.settle("unknown-tok", proxypb.SettleMessageCommand_SETTLE_ACTION_ACK)
	sess2.settle(sess2.md().GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE)
	pushFakeMsg(f2, "a-held", "m-held", "3", false, 0)
	_ = sess2.md()
	req(t, sess2.stop().GetTotalDelivered() == 9 && ackAttempt.Load() >= 2, "sess2")
	for in, want := range map[pubsub.AcknowledgeStatus]proxypb.SettlementResultEvent_AckStatus{pubsub.AcknowledgeStatusSuccess: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, pubsub.AcknowledgeStatusPermissionDenied: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, pubsub.AcknowledgeStatusFailedPrecondition: proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, pubsub.AcknowledgeStatusInvalidAckID: proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, pubsub.AcknowledgeStatusOther: proxypb.SettlementResultEvent_ACK_STATUS_OTHER, pubsub.AcknowledgeStatus(99): proxypb.SettlementResultEvent_ACK_STATUS_UNKNOWN} {
		req(t, mapAckStatus(in) == want, "mapAckStatus(%v)", in)
	}
	ps := pstest.NewServer()
	conn, err := grpc.NewClient(ps.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	noErr(t, err, "dial pstest")
	t.Cleanup(func() { _ = conn.Close(); _ = ps.Close() })
	const topicName, subName = "projects/p-test/topics/t-test", "projects/p-test/subscriptions/s-test"
	_, _ = pubsubpb.NewPublisherClient(conn).CreateTopic(e.ctx, &pubsubpb.Topic{Name: topicName})
	_, _ = pubsubpb.NewSubscriberClient(conn).CreateSubscription(e.ctx, &pubsubpb.Subscription{Name: subName, Topic: topicName, EnableExactlyOnceDelivery: true})
	req(t, e.pubOnce(&proxypb.CreatePublisherRequest{Endpoint: ps.Addr, Topic: topicName, CountThreshold: proto.Int64(1)}, "pstest").GetMessageId() != "", "pstest Publish")
	psSess := e.startSubCtx(e.ctx, &proxypb.StartSubscriberCommand{Endpoint: ps.Addr, Subscription: subName, AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT, MaxExtensionMs: proto.Int64(5000)})
	req(t, string(psSess.md().GetData()) == "pstest" && psSess.sr().GetStatus() == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS && psSess.stop().GetTotalDelivered() == 1, "pstest")
}

type failSendStream struct {
	grpc.BidiStreamingServer[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
}
type mockPullStream struct {
	grpc.ClientStream
	err error
}

func (failSendStream) Send(*proxypb.SubscriberEvent) error {
	return status.Error(codes.Unavailable, "send err")
}
func (failSendStream) Recv() (*proxypb.SubscriberCommand, error) { return nil, io.EOF }
func (m mockPullStream) RecvMsg(any) error                       { return m.err }

func TestEODDropsDuplicateMsgIDAndConcurrentStreams(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	var transientOnce atomic.Bool
	dropErrs := map[string]error{"ack-drop-ei": eiInvalidArg("ack-drop-ei", "PERMANENT_FAILURE_INVALID_ACK_ID"), "ack-drop-fp": status.Error(codes.FailedPrecondition, "perm non-errorinfo")}
	f1, sess1 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "eod-drop", AutoSettleMode: autoAck}, fakeHandlers{modHandler: func(_ context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if id := r.GetAckIds()[0]; dropErrs[id] != nil {
			return nil, dropErrs[id]
		} else if id == "ack-trans" && transientOnce.CompareAndSwap(false, true) {
			return nil, eiInvalidArg("ack-trans", "TRANSIENT_FAILURE_UNSPECIFIED")
		}
		return &emptypb.Empty{}, nil
	}})
	attrs := map[string]string{"ak": "av", "bk": "bv"}
	pushFakeBatch(f1, true, mkRM("ack-drop-ei", "dup-1", "same-body", 1, sharedPT, "", attrs))
	pushFakeBatch(f1, true, mkRM("ack-drop-fp", "dup-1", "same-body", 1, sharedPT, "", attrs))
	waitModAck(t, f1, -1, "ack-drop-ei", "ack-drop-fp")
	pushFakeBatch(f1, false, mkRM("ack-ok", "dup-1", "same-body", 1, sharedPT, "", attrs))
	req(t, !sess1.md().GetIsExactlyOnce(), "expected EOD=false after permanent drops")
	pushFakeBatch(f1, true, mkRM("ack-trans", "m-tr", "tr", 1, nil, "", attrs))
	req(t, sess1.md().GetIsExactlyOnce(), "expected EOD=true after transient retry")
	_ = sess1.stop()
	f2, sess2 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "eod-conc", AutoSettleMode: autoAck, NumStreams: proto.Int32(2)})
	for i := range 24 {
		eod := i%2 == 1
		pushFakeBatch(f2, eod, mkRM(fmt.Sprintf("a-%d", i), "same-id", fmt.Sprintf("eod=%v-%d", eod, i), int32(i%3+1), nil, "", attrs))
	}
	for range 24 {
		md := sess2.md()
		req(t, md.GetIsExactlyOnce() == strings.HasPrefix(string(md.GetData()), "eod=true-"), "concurrent dup %q", md.GetData())
	}
	req(t, sess2.stop().GetTotalDelivered() == 24, "TotalDelivered")
	f3, sess3 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "eod-fc-drop", AutoSettleMode: interactive, MaxOutstandingMessages: proto.Int64(1), ShutdownBehavior: nackBhv})
	pushFakeBatch(f3, true, mkRM("fc-1", "fc-m1", "1", 0, nil, "", nil), mkRM("fc-2", "fc-m2", "2", 0, nil, "", nil))
	_ = sess3.md()
	_ = sess3.stop()
	uSess := newSubscriberSession(nil, &proxypb.StartSubscriberCommand{}, func() {})
	clm := uSess.claimModAckBatch
	fin := func(e error, ids ...string) { uSess.finishModAckBatch(clm(modReq(60, ids...)), e) }
	rm := &pubsubpb.ReceivedMessage{AckId: "dup-ack", DeliveryAttempt: -1, Message: &pubsubpb.PubsubMessage{MessageId: "m", Data: []byte("d"), PublishTime: sharedPT}}
	pm, one := &pubsub.Message{ID: "m", Data: []byte("d"), PublishTime: sharedPT.AsTime()}, 1
	rec := func(sid int64, eod bool, rms ...*pubsubpb.ReceivedMessage) { uSess.recordReceivedEOD(sid, rms, eod) }
	trDup := eiInvalidArg("dup-ack", "TRANSIENT_X")
	rec(1, true, rm)
	c1 := uSess.claimModAckBatch(modReq(60, "dup-ack"))
	rec(2, false, rm)
	rec(3, true, rm)
	c3 := uSess.claimModAckBatch(modReq(60, "dup-ack"))
	req(t, len(c1) == 1 && c1[0].inFlight && !c1[0].superseded && !uSess.popReceivedEOD(pm) && len(c3) == 1 && c3[0] != c1[0] && c3[0].streamID == 3 && uSess.claimModAckBatch(modReq(60, "dup-ack")) == nil && c1[0].attempts == 1, "inFlight")
	uSess.finishModAckBatch(c3, eiInvalidArg("dup-ack", "PERMANENT_FAILURE_INVALID_ACK_ID"))
	req(t, uSess.claimModAckBatch(modReq(60, "dup-ack")) == nil && len(uSess.eodByAckID["dup-ack"]) == 1, "perm c3 preserves c1")
	uSess.finishModAckBatch(c1, trDup)
	fin(nil, "dup-ack")
	req(t, uSess.popReceivedEOD(pm) && len(uSess.eodByAckID) == 0 && len(uSess.eodByFP) == 0, "cross-stream recover")
	rec(7, true, rmPT("x1", "mx1", "x1", ""), rmPT("x2", "mx2", "x2", ""))
	cx := uSess.claimModAckBatch(modReq(60, "x2"))
	req(t, len(cx) == 1 && uSess.claimModAckBatch(modReq(60, "x1", "x2")) == nil, "partial-batch inFlight")
	uSess.finishModAckBatch(cx, eiInvalidArg("x2", "PERMANENT_FAILURE_INVALID_ACK_ID"))
	fin(eiInvalidArg("x1", "PERMANENT_FAILURE_INVALID_ACK_ID"), "x1")
	chkSup := func(other bool, next *pubsubpb.ReceivedMessage, err error) {
		rec(1, true, rm)
		c1 = uSess.claimModAckBatch(modReq(60, "dup-ack"))
		rec(1, false, next)
		req(t, c1[0].superseded && len(uSess.eodByAckID) == 0, "same-stream supersede")
		uSess.finishModAckBatch(c1, err)
		req(t, !uSess.popReceivedEOD(pm) && (!other || !uSess.popReceivedEOD(&pubsub.Message{ID: "mo", Data: []byte("o"), PublishTime: sharedPT.AsTime(), DeliveryAttempt: &one})) && len(uSess.eodByFP) == 0, "superseded finish %v", other)
	}
	chkSup(false, rm, nil)
	chkSup(true, rmPT("other", "mo", "o", ""), nil)
	chkSup(false, rm, trDup)
	rec(1, true, rm)
	req(t, !uSess.popReceivedEOD(pm), "pre-admission")
	fin(trDup, "dup-ack")
	req(t, !uSess.eodByAckID["dup-ack"][0].ready && !uSess.eodByAckID["dup-ack"][0].inFlight && uSess.eodByAckID["dup-ack"][0].attempts == 1, "transient")
	fin(nil, "dup-ack")
	req(t, len(uSess.eodByAckID) == 0, "ready unlinked")
	uSess.finishModAckBatch(uSess.claimModAckBatch(modReq(0, "dup-ack")), nil)
	req(t, uSess.popReceivedEOD(pm) && len(uSess.eodByFP) == 0, "AckDeadline=0 preserve ready")
	rec(1, true, rm)
	uSess.finishModAckBatch(uSess.claimModAckBatch(modReq(0, "dup-ack")), nil)
	req(t, uSess.claimModAckBatch(modReq(0, "dup-ack")) == nil && uSess.claimModAckBatch(modReq(10, "dup-ack")) == nil && len(uSess.eodByAckID["dup-ack"]) == 1 && len(uSess.eodByFP) == 0, "AckDeadline=0/10 preserve idle")
	rec(1, true, rm)
	fin(eiInvalidArg("dup-ack", "PERMANENT_FAILURE_INVALID_ACK_ID"), "dup-ack")
	req(t, len(uSess.eodByAckID) == 0 && len(uSess.eodByFP) == 0, "perm eviction")
	rec(1, true, rmPT("s-old", "mo", "o", ""))
	fin(eiInvalidArg("s-old", "TRANSIENT_X"), "s-old")
	rec(1, true, rmPT("s-new", "mn", "n", ""))
	req(t, len(uSess.eodByAckID["s-old"]) == 0 && len(uSess.eodByAckID["s-new"]) == 1, "stale")
	rec(2, false, rmPT("s-new", "mn", "n", ""))
	req(t, len(uSess.eodByAckID["s-new"]) == 1 && !uSess.popReceivedEOD(&pubsub.Message{ID: "mn", Data: []byte("n"), PublishTime: sharedPT.AsTime(), DeliveryAttempt: &one}), "cross-stream preserve")
	rec(1, false, rmPT("s-new", "mn", "n", ""))
	req(t, len(uSess.eodByAckID) == 0 && !uSess.popReceivedEOD(&pubsub.Message{ID: "mn", Data: []byte("n"), PublishTime: sharedPT.AsTime(), DeliveryAttempt: &one}), "same-stream supersede idle")
	rec(1, true, rm)
	uSess.eodClosed = true
	fin(nil, "dup-ack")
	req(t, len(uSess.eodByFP) == 0, "eodClosed")
	uSess.eodClosed = false
	rec(3, true, rmPT("a", "ma", "a", ""), rmPT("b", "mb", "b", ""))
	rec(4, true, rmPT("a", "ma", "a", ""))
	req(t, uSess.claimModAckBatch(modReq(60, "held", "a")) == nil && uSess.claimModAckBatch(modReq(60, "a", "held")) == nil && uSess.claimModAckBatch(modReq(60, "a"))[0].streamID == 4 && uSess.claimModAckBatch(nil) == nil, "rank/nil")
	rec(5, true, rmPT("p1", "mp1", "p1", ""), rmPT("p2", "mp2", "p2", ""), rmPT("p2", "mp2", "p2", ""))
	cP := uSess.claimModAckBatch(modReq(60, "p1", "p2"))
	stMix, _ := status.New(codes.InvalidArgument, "mix").WithDetails(&errdetails.ErrorInfo{Metadata: map[string]string{"p1": "TRANSIENT_X", "p2": "PERMANENT_X"}})
	uSess.finishModAckBatch(cP, stMix.Err())
	rec(6, true, rmPT("p1", "mp1", "p1", ""), rmPT("p3", "mp3", "p3", ""))
	req(t, *cP[0].rem == 1 && uSess.claimModAckBatch(modReq(60, "p1"))[0].streamID == 5 && uSess.claimModAckBatch(modReq(60, "p1", "p3"))[0].streamID == 6, "FINDING-2 exact>attempts")
	uSess.finishModAckBatch(nil, nil)
	uSess.unlinkEODEntryLocked(&eodEntry{})
	rec(1, true, rm)
	req(t, (&eodStreamWrapper{ClientStream: mockPullStream{}, sess: uSess, streamID: 1}).RecvMsg(&pubsubpb.StreamingPullResponse{}) == nil && len(uSess.eodByAckID["dup-ack"]) == 0 && len(uSess.eodByAckID["a"]) == 2, "RecvMsg keepalive purge")
	rec(1, true, rm)
	cErr := uSess.claimModAckBatch(modReq(60, "dup-ack"))
	req(t, errors.Is((&eodStreamWrapper{ClientStream: mockPullStream{err: io.EOF}, sess: uSess, streamID: 1}).RecvMsg(&pubsubpb.StreamingPullResponse{}), io.EOF), "RecvMsg err")
	uSess.finishModAckBatch(cErr, nil)
	req(t, cErr[0].superseded && len(uSess.eodByAckID["dup-ack"]) == 0 && len(uSess.eodByFP) == 0, "RecvMsg err purge")
	maxDur := time.Duration(math.MaxInt64)
	for _, tc := range []struct {
		minMs, maxMs *int64
		want         int32
	}{{nil, nil, 60}, {proto.Int64(0), nil, 60}, {proto.Int64(-1), nil, 60}, {proto.Int64(500), nil, 0}, {proto.Int64(2000), proto.Int64(2000), 2}, {proto.Int64(15000), proto.Int64(5000), 15}, {proto.Int64(math.MaxInt64), nil, int32(maxDur / time.Second)}} {
		ms := newSubscriberSession(nil, &proxypb.StartSubscriberCommand{MinDurationPerAckExtensionMs: tc.minMs, MaxDurationPerAckExtensionMs: tc.maxMs}, func() {})
		ms.recordReceivedEOD(1, []*pubsubpb.ReceivedMessage{rm}, true)
		req(t, ms.minEODDeadlineSec == tc.want && ms.claimModAckBatch(modReq(0, "dup-ack")) == nil && ms.claimModAckBatch(modReq(max(tc.want, 1)-1, "dup-ack")) == nil && len(ms.claimModAckBatch(modReq(max(tc.want, 1), "dup-ack"))) == 1, "minEOD(%v)=%d", tc.minMs, ms.minEODDeadlineSec)
	}
	for _, tc := range []struct {
		rc  []int32
		c   codes.Code
		mix bool
		n   int
	}{{nil, codes.Unavailable, true, 2}, {nil, codes.Internal, true, 1}, {[]int32{int32(codes.DataLoss)}, codes.Unavailable, true, 1}, {[]int32{int32(codes.DataLoss)}, codes.DataLoss, true, 2}, {nil, codes.Internal, false, 2}, {nil, codes.InvalidArgument, false, 0}} {
		ts, st := newSubscriberSession(nil, &proxypb.StartSubscriberCommand{RetryCodes: tc.rc}, func() {}), status.New(tc.c, "m")
		if tc.mix {
			st, _ = st.WithDetails(&errdetails.ErrorInfo{Metadata: map[string]string{"a": "TRANSIENT_X", "b": "PERMANENT_X"}})
		}
		ts.recordReceivedEOD(1, []*pubsubpb.ReceivedMessage{rmPT("a", "ma", "a", ""), rmPT("b", "mb", "b", "")}, true)
		ts.finishModAckBatch(ts.claimModAckBatch(modReq(60, "a", "b")), st.Err())
		req(t, len(ts.eodByAckID) == tc.n, "twoSet(%v,%v)=%d", tc.rc, tc.c, len(ts.eodByAckID))
	}
	close(uSess.receiveExitedCh)
	uSess.handleMessage(e.ctx, pm)
	assertCode(t, uSess.sendLiveEvent(&proxypb.SubscriberEvent{}), codes.Canceled)
	fSess := newSubscriberSession(failSendStream{}, &proxypb.StartSubscriberCommand{}, func() {})
	fSess.handleMessage(e.ctx, pm)
	req(t, uSess.totalDelivered.Load() == 0 && fSess.totalDelivered.Load() == 0 && waitCh(e.ctx, fSess.sessionAbortCh) == nil && fSess.sendStopped(0, "") != nil && fSess.sendStopped(0, "") == nil, "failSendStream")
	assertCode(t, fSess.sendLiveEvent(&proxypb.SubscriberEvent{}), codes.Canceled)
	uSess.settleAndEmitResult("t", &pubsub.AckResult{}, false)
	fSess.settleAndEmitResult("t", &pubsub.AckResult{}, false)
}

func TestRetryOverflowAndEODEdgeCases(t *testing.T) {
	r := buildCustomRetryFactory([]codes.Code{codes.DataLoss}, gax.Backoff{Initial: 10, Max: 20, Multiplier: math.NaN()}, nil, proto.Int64(-5), proto.Int64(math.MaxInt64), proto.Float64(math.Inf(1)), proto.Int32(150))()
	d, ok := r.Retry(errors.New("raw"))
	d0, ok0 := buildCustomRetryFactory([]codes.Code{codes.DataLoss}, defaultPublishBackoff, nil, proto.Int64(0), proto.Int64(0), nil, nil)().Retry(status.Error(codes.DataLoss, "x"))
	req(t, !ok && d == 0 && ok0 && d0 > 0 && d0 <= time.Second, "raw/r0 Retry")
	r = buildCustomRetryFactory([]codes.Code{codes.DataLoss}, defaultPublishBackoff, nil, proto.Int64(1), proto.Int64(math.MaxInt64), proto.Float64(1e300), proto.Int32(0))()
	for i := range 100 {
		d, ok := r.Retry(status.Error(codes.DataLoss, "x"))
		req(t, ok && d > 0, "overflow %d", i)
	}
	e := newTestEnv(t, 60*time.Second)
	f1, addr1 := startFakePubSubServer(t)
	var pubTries, subTries atomic.Int32
	f1.pubHandler = func(_ context.Context, _ *pubsubpb.PublishRequest, _ metadata.MD) (*pubsubpb.PublishResponse, error) {
		if pubTries.Add(1) < 3 {
			return nil, status.Error(codes.DataLoss, "retry-big")
		}
		return &pubsubpb.PublishResponse{MessageIds: []string{"big-ok"}}, nil
	}
	for _, mult := range []float64{2.0, 1e300} {
		pubTries.Store(0)
		req(t, (mult < 10) == (e.pubOnce(&proxypb.CreatePublisherRequest{Endpoint: addr1, Topic: "projects/p/topics/big", CountThreshold: proto.Int64(1), TotalTimeoutMs: proto.Int64(map[bool]int64{true: 500, false: 40}[mult < 10]), RetryCodes: []int32{int32(codes.DataLoss)}, InitialRetryDelayMs: proto.Int64(1), MaxRetryDelayMs: proto.Int64(math.MaxInt64), RetryDelayMultiplier: proto.Float64(mult)}, "big").GetErrorCode() == 0), "mult=%v", mult)
	}
	a2PermDone := make(chan struct{})
	closeA2 := once(a2PermDone)
	f1.ackHandler = func(_ context.Context, _ *pubsubpb.AcknowledgeRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if subTries.Add(1) < 3 {
			return nil, status.Error(codes.DataLoss, "retry-sub-big")
		}
		return &emptypb.Empty{}, nil
	}
	f1.modHandler = func(_ context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if r.GetAckDeadlineSeconds() > 0 && len(r.GetAckIds()) == 1 && r.GetAckIds()[0] == "ack-a2-dup" {
			defer closeA2()
			return nil, eiInvalidArg("ack-a2-dup", "PERMANENT_FAILURE_INVALID_ACK_ID")
		}
		return &emptypb.Empty{}, nil
	}
	sBig := e.startSubCtx(e.ctx, &proxypb.StartSubscriberCommand{Endpoint: addr1, Subscription: "projects/p/subscriptions/s-big", NumStreams: proto.Int32(1), RetryCodes: []int32{int32(codes.DataLoss)}, InitialRetryDelayMs: proto.Int64(1), MaxRetryDelayMs: proto.Int64(math.MaxInt64), RetryDelayMultiplier: proto.Float64(2.0)})
	pushFakeBatch(f1, true, rmPT("ack-dup-same", "msg-dup", "dup", ""), rmPT("ack-dup-same", "msg-dup", "dup", ""))
	req(t, sBig.ackNext().GetIsExactlyOnce() && sBig.ackNext().GetIsExactlyOnce(), "dup ack_id EOD=true")
	f1.waitCall(t, func(_ []fakePubSubCall) bool { return subTries.Load() >= 3 })
	pushFakeBatch(f1, true, mkRM("ack-k0", "m-k0", "k0", 0, sharedPT, "k", nil), mkRM("ack-dup-ord", "m-dup-ord", "ord", 0, sharedPT, "k", nil))
	mk0 := sBig.md()
	pushFakeBatch(f1, false, mkRM("ack-dup-ord", "m-dup-ord", "ord", 0, sharedPT, "k", nil))
	sBig.ack(mk0)
	req(t, sBig.ackNext().GetIsExactlyOnce() && !sBig.ackNext().GetIsExactlyOnce(), "queued ord dup")
	pushFakeBatch(f1, true, rmPT("ack-a2-k0", "m-a2-k0", "k0", "k"), rmPT("ack-a2-dup", "m-a2-1", "a2-1", "k"))
	ma2k0 := sBig.md()
	pushFakeBatch(f1, true, rmPT("ack-a2-dup", "m-a2-2", "a2-2", "k"))
	<-a2PermDone
	<-time.After(20 * time.Millisecond)
	sBig.ack(ma2k0)
	ma2_1 := sBig.ackNext()
	req(t, ma2_1.GetIsExactlyOnce() && ma2_1.GetMessageId() == "m-a2-1", "A2: %+v", ma2_1)
	pushFakeBatch(f1, true, rmPT("ack-a3-k0", "m-a3-k0", "k0", "k1"), rmPT("ack-a3-dup", "m-a3-1", "a3-1", "k1"))
	ma3k0 := sBig.md()
	pushFakeBatch(f1, false, rmPT("ack-a3-dup", "m-a3-2", "a3-2", "k2"))
	ma3_2 := sBig.md()
	sBig.settle(ma3_2.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK)
	waitModAck(t, f1, 0, "ack-a3-dup")
	sBig.ack(ma3k0)
	ma3_1 := sBig.ackNext()
	req(t, !ma3_2.GetIsExactlyOnce() && ma3_1.GetIsExactlyOnce() && ma3_1.GetMessageId() == "m-a3-1", "A3: %+v", ma3_1)
	_ = sBig.stop()
	sendPull := func(ch <-chan struct{}, resps ...*pubsubpb.StreamingPullResponse) func(pubsubpb.Subscriber_StreamingPullServer) {
		return func(srv pubsubpb.Subscriber_StreamingPullServer) {
			_ = srv.Send(resps[0])
			if len(resps) > 1 && waitCh(srv.Context(), ch) == nil {
				_ = srv.Send(resps[1])
			}
		}
	}
	runTwoStreamGate(t, e, "ms", autoAck, "ack-s1-perm", eiInvalidArg("ack-s1-perm", "PERMANENT_FAILURE_INVALID_ACK_ID"), sendPull(nil, pullResp(true, rmPT("ack-s1-perm", "msg-ms", "ms", ""))), sendPull(nil, pullResp(false, rmPT("ack-s2-ne", "msg-ms", "ms", ""))), func(_ func(), s *subSess, _ <-chan struct{}) { req(t, !s.md().GetIsExactlyOnce(), "ms stole EOD") }, 1)
	a4Next := make(chan struct{})
	runTwoStreamGate(t, e, "a4", autoAck, "ack-a4-dup", eiInvalidArg("ack-a4-dup", "PERMANENT_FAILURE_INVALID_ACK_ID"), sendPull(nil, pullResp(true, rmPT("ack-a4-dup", "msg-a4", "ms", ""))), sendPull(a4Next, pullResp(true, rmPT("ack-a4-dup", "msg-a4", "ms", "")), pullResp(false, rmPT("ack-a4-b", "msg-a4", "ms", ""))), func(_ func(), s *subSess, _ <-chan struct{}) {
		req(t, s.md().GetIsExactlyOnce(), "A4 1")
		close(a4Next)
		req(t, !s.md().GetIsExactlyOnce(), "A4 poisoned")
	}, 2)
	permA1 := eiInvalidArg("ack-a1-dup", "PERMANENT_FAILURE_INVALID_ACK_ID")
	runA1 := func(name, m1 string, gateErr error) {
		m0Held := make(chan struct{})
		runTwoStreamGate(t, e, "a1", interactive, "ack-a1-dup", gateErr, sendPull(m0Held, pullResp(true, rmPT("ack-a1-k0", "a1-k0", "k0", "k")), pullResp(true, rmPT("ack-a1-dup", m1, m1, "k"))), sendPull(nil, pullResp(gateErr != nil, rmPT("ack-a1-dup", "a1-m2", "a1-m2", "k"))), func(closeGate func(), s *subSess, s2Done <-chan struct{}) {
			md0 := s.md()
			close(m0Held)
			<-s2Done
			<-time.After(20 * time.Millisecond)
			closeGate()
			<-time.After(30 * time.Millisecond)
			s.ack(md0)
			if gateErr == nil {
				req(t, !s.ackNext().GetIsExactlyOnce(), "TwoStreamSupErrNil Batch2 EOD=false")
			}
			got2 := s.ackNext()
			req(t, md0.GetIsExactlyOnce() && got2.GetIsExactlyOnce() && got2.GetMessageId() == "a1-m2", "%s: %+v", name, got2)
		}, map[bool]int64{true: 2, false: 3}[gateErr != nil])
	}
	runA1("A1_sameFP=false", "a1-m1", permA1)
	runA1("A1_sameFP=true", "a1-m2", permA1)
	runA1("TwoStreamSupErrNil", "a1-m2", nil)
	runTwoStreamGate(t, e, "s2-tr", autoAck, "ack-s2tr", eiInvalidArg("ack-s2tr", "TRANSIENT_FAILURE_UNSPECIFIED"), sendPull(nil, pullResp(true, rmPT("ack-s2tr", "m-tr-eod", "tr-eod", ""))), sendPull(nil, pullResp(false, rmPT("ack-s2tr", "m-tr-alo", "tr-alo", ""))), func(closeGate func(), s *subSess, s2Done <-chan struct{}) {
		<-s2Done
		closeGate()
		m1, m2 := s.md(), s.md()
		byID := map[string]bool{m1.GetMessageId(): m1.GetIsExactlyOnce(), m2.GetMessageId(): m2.GetIsExactlyOnce()}
		req(t, !byID["m-tr-alo"] && byID["m-tr-eod"], "NumStreams2_Transient: %v", byID)
	}, 2)
	for _, tc := range []struct {
		name  string
		mode  proxypb.StartSubscriberCommand_AutoSettleMode
		failN int64
		init  *int64
		err   error
	}{{
		"D1a_TransientBO", autoAck, 3, nil, eiInvalidArg("ack-d1", "TRANSIENT_FAILURE_UNSPECIFIED")},
		{"D1b_GaxUnavailBO", autoAck, 1, proto.Int64(1500), status.Error(codes.Unavailable, "u")},
		{"D1c_CrossNackBO", autoNack, 3, nil, eiInvalidArg("ack-d1", "TRANSIENT_FAILURE_UNSPECIFIED")},
	} {
		s2Go, tries := make(chan struct{}), atomic.Int64{}
		f, s := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "d1-" + tc.name, AutoSettleMode: tc.mode, NumStreams: proto.Int32(2), InitialRetryDelayMs: tc.init, MaxRetryDelayMs: tc.init}, fakeHandlers{
			modHandler: func(_ context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
				if r.GetAckDeadlineSeconds() >= 60 && slices.Contains(r.GetAckIds(), "ack-d1") && tries.Add(1) <= tc.failN {
					if tries.Load() == tc.failN {
						close(s2Go)
					}
					return nil, tc.err
				}
				return &emptypb.Empty{}, nil
			},
			pullHandler: func(st pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
				if seq == 1 {
					_ = st.Send(pullResp(true, rmPT("ack-d1", "m-eod", "1", "")))
				} else if seq == 2 && waitCh(st.Context(), s2Go) == nil {
					_ = st.Send(pullResp(false, rmPT("ack-d1", "m-alo", "2", "")))
				}
				return nil
			},
		})
		m1, m2 := s.md(), s.md()
		if tc.mode == autoNack {
			waitModAck(t, f, 0, "ack-d1")
		}
		byID := map[string]bool{m1.GetMessageId(): m1.GetIsExactlyOnce(), m2.GetMessageId(): m2.GetIsExactlyOnce()}
		req(t, byID["m-eod"] && !byID["m-alo"] && s.stop().GetTotalDelivered() == 2, "%s: %v", tc.name, byID)
	}
	d2S1, d2Perm, d2B3, d2Tries := make(chan struct{}), make(chan struct{}), make(chan struct{}), atomic.Int32{}
	closeD2Perm := once(d2Perm)
	fD2, sD2 := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "d2", AutoSettleMode: autoAck, NumStreams: proto.Int32(2)}, fakeHandlers{
		modHandler: func(_ context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
			if r.GetAckDeadlineSeconds() >= 60 && slices.Contains(r.GetAckIds(), "ack-d2") {
				if n := d2Tries.Add(1); n <= 3 {
					if n == 2 {
						close(d2S1)
					}
					return nil, eiInvalidArg("ack-d2", "TRANSIENT_FAILURE_UNSPECIFIED")
				}
				defer closeD2Perm()
				return nil, eiInvalidArg("ack-d2", "PERMANENT_FAILURE_INVALID_ACK_ID")
			}
			return &emptypb.Empty{}, nil
		},
		pullHandler: func(st pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
			if seq == 2 {
				_ = st.Send(pullResp(true, rmPT("ack-d2", "m-d2-drop", "2", "")))
			} else if seq == 1 && waitCh(st.Context(), d2S1) == nil {
				sendPull(d2B3, pullResp(false, rmPT("ack-d2", "m-d2-1", "1", "")), pullResp(false, rmPT("ack-d2-b3", "m-d2-drop", "2", "")))(st)
			}
			return nil
		},
	})
	<-d2Perm
	waitModAck(t, fD2, 10, "ack-d2")
	close(d2B3)
	m1, m2 := sD2.md(), sD2.md()
	req(t, !m1.GetIsExactlyOnce() && !m2.GetIsExactlyOnce() && sD2.stop().GetTotalDelivered() == 2, "D2: %+v %+v", m1, m2)
	var gaxAtt atomic.Int32
	stGax, _ := status.New(codes.Unavailable, "u").WithDetails(&errdetails.ErrorInfo{Metadata: map[string]string{"ag1": "TRANSIENT_X", "ag2": "PERMANENT_FAILURE_INVALID_ACK_ID"}})
	fG, sG := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "gax-ei", AutoSettleMode: autoAck, NumStreams: proto.Int32(1), InitialRetryDelayMs: proto.Int64(5), MaxRetryDelayMs: proto.Int64(10)}, fakeHandlers{modHandler: func(_ context.Context, _ *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
		if gaxAtt.Add(1) == 1 {
			return nil, stGax.Err()
		}
		return &emptypb.Empty{}, nil
	}})
	pushFakeBatch(fG, true, rmPT("ag1", "mg1", "1", ""), rmPT("ag2", "mg2", "2", ""))
	gm1, gm2 := sG.md(), sG.md()
	req(t, gm1.GetIsExactlyOnce() && gm2.GetIsExactlyOnce() && sG.stop().GetTotalDelivered() == 2, "GaxRetry: %+v %+v", gm1, gm2)
}

func TestEODExhaustionAndKaTickSuperseded(t *testing.T) {
	e := newTestEnv(t, 75*time.Second)
	waitEOD := func(subT *testing.T, fn func() bool, msg string) {
		subT.Helper()
		for !fn() {
			req(subT, e.ctx.Err() == nil, "timeout %s: %v", msg, e.ctx.Err())
			time.Sleep(200 * time.Microsecond)
		}
	}
	t.Run("ExhaustIdle", func(subT *testing.T) {
		subT.Parallel()
		var mods atomic.Int32
		f, s := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: "ex-idle", AutoSettleMode: autoAck}, fakeHandlers{modHandler: func(_ context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
			if r.GetAckDeadlineSeconds() >= 60 && slices.Contains(r.GetAckIds(), "ack-ex") {
				mods.Add(1)
				return nil, eiInvalidArg("ack-ex", "TRANSIENT_FAILURE_UNSPECIFIED")
			}
			return &emptypb.Empty{}, nil
		}})
		pushFakeBatch(f, true, rmPT("ack-ex", "msg-ex", "ex", ""))
		f.waitCall(subT, func(_ []fakePubSubCall) bool { return mods.Load() >= 5 })
		pushFakeBatch(f, false, rmPT("ack-ex", "msg-ex", "ex", ""))
		pushFakeBatch(f, false, rmPT("ack-ex-3", "msg-ex", "ex", ""))
		req(subT, !s.md().GetIsExactlyOnce() && !s.md().GetIsExactlyOnce() && s.stop().GetTotalDelivered() == 2, "ExhaustIdle")
	})
	for _, tc := range []struct {
		name         string
		sameAck, err bool
	}{{"KaSameAck", true, false}, {"KaDistinctAck", false, false}, {"KaRecvMsgErr", false, true}} {
		t.Run(tc.name, func(subT *testing.T) {
			subT.Parallel()
			runTry := func(attempt int) bool {
				b1Ack := fmt.Sprintf("ack-%s-%d", tc.name, attempt)
				b2Ack := map[bool]string{true: b1Ack, false: fmt.Sprintf("ack-b2-%s-%d", tc.name, attempt)}[tc.sameAck]
				ackBlk, ackGate, att1Done, kaEnt, kaGate, loserDone, seq2Ch := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				closeAckBlk, closeAckGate, closeKaGate, closeLoser, closeSeq2 := once(ackBlk), once(ackGate), once(kaGate), once(loserDone), once(seq2Ch)
				var tStreamNano, winnerAtt atomic.Int64
				var att1Once, winnerTaken, doubleClaimed atomic.Bool
				var sessPtr atomic.Pointer[subscriberSession]
				var winEntry atomic.Pointer[eodEntry]
				subCtx, subCancel := context.WithCancel(e.ctx)
				defer func() { subCancel(); closeAckGate(); closeKaGate() }()
				f, s := e.openFakeSub(subCtx, &proxypb.StartSubscriberCommand{Subscription: fmt.Sprintf("ex-%s-%d", tc.name, attempt), AutoSettleMode: interactive, NumStreams: proto.Int32(1), RetryCodes: []int32{int32(codes.DataLoss)}, InitialRetryDelayMs: proto.Int64(6000), MaxRetryDelayMs: proto.Int64(6000), RetryDelayMultiplier: proto.Float64(1.0)}, fakeHandlers{
					pullHandler: func(_ pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
						if seq == 1 {
							tStreamNano.CompareAndSwap(0, time.Now().UnixNano())
						} else if seq == 2 {
							closeSeq2()
						}
						return nil
					},
					ackHandler: func(actx context.Context, r *pubsubpb.AcknowledgeRequest, _ metadata.MD) (*emptypb.Empty, error) {
						if slices.Contains(r.GetAckIds(), "ack-warm") {
							closeAckBlk()
							return &emptypb.Empty{}, waitCh(actx, ackGate)
						}
						return &emptypb.Empty{}, nil
					},
					modHandler: func(mctx context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
						if r.GetAckDeadlineSeconds() < 60 || !slices.Contains(r.GetAckIds(), b1Ack) {
							return &emptypb.Empty{}, nil
						}
						if att1Once.CompareAndSwap(false, true) {
							if d := 5050*time.Millisecond - time.Since(time.Unix(0, tStreamNano.Load())); d > 0 {
								tm := time.NewTimer(d)
								defer tm.Stop()
								if err := waitCh(mctx, tm.C); err != nil {
									return nil, err
								}
							}
							defer close(att1Done)
							return nil, status.Error(codes.DataLoss, "att1")
						}
						ms := sessPtr.Load()
						if ms == nil {
							return nil, status.Error(codes.DataLoss, "pre-gate")
						}
						ms.eodMu.Lock()
						q, won := ms.eodByAckID[b1Ack], winnerTaken.CompareAndSwap(false, true)
						if won && len(q) == 1 && q[0].inFlight && winnerAtt.CompareAndSwap(int64(q[0].attempts)-1, int64(q[0].attempts)) {
							winEntry.Store(q[0])
						} else if !won && (len(q) != 1 || !q[0].inFlight || int64(q[0].attempts) != winnerAtt.Load()) || (won && len(q) == 1 && q[0].inFlight) {
							doubleClaimed.Store(true)
						}
						ms.eodMu.Unlock()
						if !won {
							defer closeLoser()
							return nil, eiInvalidArg(b1Ack, "PERMANENT_FAILURE_INVALID_ACK_ID")
						}
						close(kaEnt)
						if winEntry.Load() == nil {
							return nil, status.Error(codes.PermissionDenied, "ka-unclaimed")
						}
						return &emptypb.Empty{}, waitCh(mctx, kaGate)
					},
				})
				pushFakeBatch(f, false, rmPT("ack-warm", "m-warm", "w", ""))
				req(subT, !s.ackNext().GetIsExactlyOnce(), "warm")
				req(subT, waitCh(e.ctx, ackBlk) == nil, "ackBlk")
				pushFakeBatch(f, true, rmPT(b1Ack, "msg-ex", "ex", ""))
				req(subT, waitCh(e.ctx, att1Done) == nil, "att1Done")
				waitEOD(subT, func() bool {
					e.srv.mu.Lock()
					defer e.srv.mu.Unlock()
					for c := range e.srv.subscribers {
						c.eodMu.Lock()
						if q := c.eodByAckID[b1Ack]; len(q) > 0 && !q[0].inFlight {
							winnerAtt.Store(int64(q[0].attempts))
							sessPtr.Store(c)
							c.eodMu.Unlock()
							return true
						}
						c.eodMu.Unlock()
					}
					return false
				}, "!inFlight")
				closeAckGate()
				req(subT, waitCh(e.ctx, kaEnt) == nil && !doubleClaimed.Load(), "kaEnt/doubleClaim")
				if winEntry.Load() == nil {
					return false
				}
				req(subT, waitCh(e.ctx, loserDone) == nil && !doubleClaimed.Load(), "loserDone/doubleClaim")
				if tc.err {
					f.pull1Err <- status.Error(codes.Unavailable, "reset")
					req(subT, waitCh(e.ctx, seq2Ch) == nil, "seq2")
				}
				pushFakeBatch(f, false, rmPT(b2Ack, map[bool]string{true: "msg-other", false: "msg-ex"}[tc.err], "ex", ""))
				req(subT, !s.ackNext().GetIsExactlyOnce(), "Batch2")
				hasB2Ack := func(cs []fakePubSubCall) bool {
					return slices.ContainsFunc(cs, func(c fakePubSubCall) bool { return c.ackReq != nil && slices.Contains(c.ackReq.GetAckIds(), b2Ack) })
				}
				ms := sessPtr.Load()
				inFlight := func() bool { ms.eodMu.Lock(); defer ms.eodMu.Unlock(); return winEntry.Load().inFlight }
				time.Sleep(150 * time.Millisecond)
				if !inFlight() || hasB2Ack(f.snapshotCalls()) {
					return false
				}
				closeKaGate()
				f.waitCall(subT, hasB2Ack)
				waitEOD(subT, func() bool { return !inFlight() }, "finishModAckBatch")
				pushFakeBatch(f, false, rmPT("ack-b3-"+tc.name, "msg-ex", "ex", ""))
				req(subT, !s.ackNext().GetIsExactlyOnce() && s.stop().GetTotalDelivered() == 3, "Batch3")
				return true
			}
			for attempt := range 3 {
				if runTry(attempt) {
					return
				}
			}
			subT.Fatalf("kaTick collision retries exhausted")
		})
	}
	t.Run("InterceptorKeepalive", func(subT *testing.T) {
		iSess := newSubscriberSession(nil, &proxypb.StartSubscriberCommand{}, func() {})
		iSess.recordReceivedEOD(1, []*pubsubpb.ReceivedMessage{rmPT("ik", "mik", "ik", "")}, true)
		kaCh, relKa, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			_ = iSess.unaryInterceptor()(e.ctx, "/google.pubsub.v1.Subscriber/ModifyAckDeadline", modReq(60, "ik"), nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				close(kaCh)
				<-relKa
				return nil
			})
		}()
		<-kaCh
		req(subT, (&eodStreamWrapper{ClientStream: mockPullStream{}, sess: iSess, streamID: 1}).RecvMsg(&pubsubpb.StreamingPullResponse{}) == nil, "keepalive RecvMsg")
		close(relKa)
		<-done
		one := 1
		req(subT, !iSess.popReceivedEOD(&pubsub.Message{ID: "mik", Data: []byte("ik"), PublishTime: sharedPT.AsTime(), DeliveryAttempt: &one}), "keepalive superseded")
	})
}
func runTwoStreamGate(t *testing.T, e *testEnv, sub string, mode proxypb.StartSubscriberCommand_AutoSettleMode, blockAckID string, gateErr error, s1Fn, s2Fn func(pubsubpb.Subscriber_StreamingPullServer), check func(closeGate func(), s *subSess, s2Done <-chan struct{}), wantTotal int64) {
	t.Helper()
	modBlocked, modGate, s2Done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var dupCalls atomic.Int32
	closeBlk, closeS2, closeGate := once(modBlocked), once(s2Done), once(modGate)
	_, s := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: sub, AutoSettleMode: mode, NumStreams: proto.Int32(2)}, fakeHandlers{
		modHandler: func(mctx context.Context, r *pubsubpb.ModifyAckDeadlineRequest, _ metadata.MD) (*emptypb.Empty, error) {
			if slices.Contains(r.GetAckIds(), blockAckID) {
				if dupCalls.Add(1) == 1 {
					closeBlk()
					return &emptypb.Empty{}, cmp.Or(waitCh(mctx, modGate), gateErr)
				}
				defer closeS2()
			}
			return &emptypb.Empty{}, nil
		},
		pullHandler: func(st pubsubpb.Subscriber_StreamingPullServer, seq int32) error {
			if seq == 1 {
				s1Fn(st)
			} else if seq == 2 && waitCh(st.Context(), modBlocked) == nil {
				s2Fn(st)
			}
			return nil
		},
	})
	check(closeGate, s, s2Done)
	closeGate()
	req(t, s.stop().GetTotalDelivered() == wantTotal, "%s TotalDelivered", sub)
}

type barrierSubStream struct {
	grpc.BidiStreamingServer[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
	ctx                      context.Context
	cmds                     chan *proxypb.SubscriberCommand
	started, inSend, release chan struct{}
	sendOnce                 sync.Once
	sentMD                   atomic.Int64
	stopped                  atomic.Pointer[proxypb.StoppedEvent]
}

func (b *barrierSubStream) Context() context.Context { return b.ctx }
func (b *barrierSubStream) Recv() (*proxypb.SubscriberCommand, error) {
	if c, ok := <-b.cmds; ok {
		return c, nil
	}
	return nil, io.EOF
}
func (b *barrierSubStream) Send(ev *proxypb.SubscriberEvent) error {
	if ev.GetStarted() != nil {
		close(b.started)
	} else if ev.GetMessageDelivered() != nil {
		b.sendOnce.Do(func() { close(b.inSend); <-b.release })
		b.sentMD.Add(1)
	} else if ev.GetStopped() != nil {
		b.stopped.Store(ev.GetStopped())
	}
	return nil
}

func TestTotalDeliveredShutdownZeroAndDurationClamping(t *testing.T) {
	const maxMs = math.MaxInt64 / int64(time.Millisecond)
	for _, tc := range []struct{ in, u, s int64 }{
		{math.MinInt64, 0, math.MinInt64}, {-maxMs - 1, 0, math.MinInt64}, {-10, 0, int64(-10 * time.Millisecond)}, {-1, 0, int64(-time.Millisecond)},
		{0, 0, 0}, {10, int64(10 * time.Millisecond), int64(10 * time.Millisecond)}, {maxMs + 1, math.MaxInt64, math.MaxInt64}, {math.MaxInt64, math.MaxInt64, math.MaxInt64},
	} {
		req(t, int64(clampMsToDuration(tc.in)) == tc.u && int64(clampSignedMsToDuration(tc.in)) == tc.s, "clamp(%d)", tc.in)
	}
	sub := &pubsub.Subscriber{}
	applySubscriberSettings(sub, &proxypb.StartSubscriberCommand{MaxExtensionMs: proto.Int64(math.MaxInt64), MinDurationPerAckExtensionMs: proto.Int64(math.MaxInt64), MaxDurationPerAckExtensionMs: proto.Int64(math.MaxInt64)})
	req(t, sub.ReceiveSettings.MaxExtension == math.MaxInt64 && sub.ReceiveSettings.MinDurationPerAckExtension == math.MaxInt64 && sub.ReceiveSettings.MaxDurationPerAckExtension == math.MaxInt64, "sub MaxInt64")
	for _, tc := range []struct {
		b        proxypb.ShutdownBehavior
		ms, want int64
	}{{waitBhv, math.MaxInt64, math.MaxInt64}, {waitBhv, -1, int64(-time.Millisecond)}, {waitBhv, math.MinInt64, math.MinInt64}, {nackBhv, math.MaxInt64, math.MaxInt64}} {
		applySubscriberSettings(sub, &proxypb.StartSubscriberCommand{MaxExtensionMs: proto.Int64(0), ShutdownBehavior: tc.b, ShutdownTimeoutMs: proto.Int64(tc.ms)})
		req(t, sub.ReceiveSettings.MaxExtension == -1 && int64(sub.ReceiveSettings.ShutdownOptions.Timeout) == tc.want, "sub timeout(%v,%d)", tc.b, tc.ms)
	}
	e := newTestEnv(t, 25*time.Second)
	_, addrPub := startFakePubSubServer(t)
	for _, tc := range []struct {
		tot, dly, hdg *int64
		wantErr       bool
	}{{proto.Int64(math.MaxInt64), nil, nil, false}, {proto.Int64(5000), proto.Int64(math.MaxInt64), nil, false},
		{proto.Int64(-10), nil, nil, true}, {proto.Int64(math.MinInt64), nil, nil, true}, {proto.Int64(5000), nil, proto.Int64(math.MaxInt64), true}, {proto.Int64(5000), nil, proto.Int64(math.MinInt64), true},
	} {
		req(t, (e.pubOnce(&proxypb.CreatePublisherRequest{Endpoint: addrPub, Topic: "projects/p/topics/t-dur", CountThreshold: proto.Int64(1), TotalTimeoutMs: tc.tot, DelayThresholdMs: tc.dly, Hedging: map[bool]*proxypb.HedgingSettings{true: {Enabled: true, DelayMs: tc.hdg}}[tc.hdg != nil]}, "dur").GetErrorCode() != 0) == tc.wantErr, "pub %v", tc)
	}
	fBar, addrBar := startFakePubSubServer(t)
	bs := &barrierSubStream{ctx: e.ctx, cmds: make(chan *proxypb.SubscriberCommand, 4), started: make(chan struct{}), inSend: make(chan struct{}), release: make(chan struct{})}
	bs.cmds <- &proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: &proxypb.StartSubscriberCommand{Endpoint: addrBar, Subscription: "projects/p/subscriptions/s-bar", AutoSettleMode: autoDrop, ShutdownBehavior: waitBhv, ShutdownTimeoutMs: proto.Int64(0)}}}
	doneErr := make(chan error, 1)
	go func() { doneErr <- e.srv.SubscriberSession(bs) }()
	<-bs.started
	pushFakeMsg(fBar, "ack-b", "m-b", "b", false, 0)
	<-bs.inSend
	e.srv.mu.Lock()
	barSess := slices.Collect(maps.Keys(e.srv.subscribers))[0]
	e.srv.mu.Unlock()
	bs.cmds <- stopSubCmd
	<-barSess.receiveExitedCh
	time.Sleep(15 * time.Millisecond)
	close(bs.release)
	close(bs.cmds)
	noErr(t, <-doneErr, "barrier")
	req(t, bs.sentMD.Load() == 1 && bs.stopped.Load().GetTotalDelivered() == 1, "barrier %d/%d", bs.sentMD.Load(), bs.stopped.Load().GetTotalDelivered())
	for _, bhv := range []proxypb.ShutdownBehavior{waitBhv, nackBhv} {
		for round := range 10 {
			fB, sB := e.openFakeSub(nil, &proxypb.StartSubscriberCommand{Subscription: fmt.Sprintf("burst-%d-%d", bhv, round), AutoSettleMode: autoDrop, NumStreams: proto.Int32(2), ShutdownBehavior: bhv, ShutdownTimeoutMs: proto.Int64(0)})
			go func() {
				for i := range 15 {
					pushFakeMsg(fB, fmt.Sprintf("ab-%d", i), fmt.Sprintf("mb-%d", i), "x", false, 0)
				}
			}()
			_ = sB.md()
			stopEv, rest := sB.stopCount()
			req(t, stopEv.GetTotalDelivered() == 1+rest, "burst %v/%d: %d!=%d", bhv, round, stopEv.GetTotalDelivered(), 1+rest)
		}
	}
	e = newTestEnv(t, 15*time.Second)
	fg, addrG := startFakePubSubServer(t)
	fg.pubHandler = func(_ context.Context, _ *pubsubpb.PublishRequest, _ metadata.MD) (*pubsubpb.PublishResponse, error) {
		time.Sleep(320 * time.Millisecond)
		return &pubsubpb.PublishResponse{MessageIds: []string{"grace-ok"}}, nil
	}
	pidG := e.createPub(&proxypb.CreatePublisherRequest{Endpoint: addrG, Topic: "projects/p/topics/t-grace", CountThreshold: proto.Int64(1), TotalTimeoutMs: proto.Int64(5000)})
	pubResCh := make(chan *proxypb.PublishResponse, 1)
	go func() {
		res, _ := e.client.Publish(e.ctx, &proxypb.PublishRequest{PublisherId: pidG, Data: []byte("g")})
		pubResCh <- res
	}()
	fg.waitCall(t, func(cs []fakePubSubCall) bool { return len(cs) >= 1 })
	_, err := e.client.ShutdownProxy(e.ctx, &proxypb.ShutdownProxyRequest{GracePeriodMs: math.MaxInt64})
	noErr(t, err, "ShutdownProxy MaxInt64")
	resG := <-pubResCh
	req(t, resG.GetErrorCode() == 0 && resG.GetMessageId() == "grace-ok", "GracePeriod MaxInt64: %+v", resG)
	e.srv.waitClosed()
}
func setupHungPub(t *testing.T, ctx context.Context, client proxypb.PubSubProxyClient, withFC bool) (*fakePubSubServer, string, string) {
	t.Helper()
	f, addr := startFakePubSubServer(t)
	f.pubHandler = func(pctx context.Context, _ *pubsubpb.PublishRequest, _ metadata.MD) (*pubsubpb.PublishResponse, error) {
		<-pctx.Done()
		return nil, pctx.Err()
	}
	pr := &proxypb.CreatePublisherRequest{Endpoint: addr, Topic: "projects/p/topics/t-hung", CountThreshold: proto.Int64(1), TotalTimeoutMs: proto.Int64(30_000)}
	if withFC {
		pr.MaxOutstandingMessages, pr.LimitExceededBehavior = proto.Int64(1), proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK
	}
	r, err := client.CreatePublisher(ctx, pr)
	noErr(t, err, "CreatePublisher hung")
	_, err = client.Publish(ctx, &proxypb.PublishRequest{PublisherId: r.GetPublisherId(), Data: []byte("1"), AsyncFire: true})
	noErr(t, err, "Publish hung")
	f.waitCall(t, func(cs []fakePubSubCall) bool { return len(cs) >= 1 })
	return f, addr, r.GetPublisherId()
}

func TestSubprocessReadyAndHungShutdown(t *testing.T) {
	e := newTestEnv(t, 15*time.Second)
	fIn, addrIn, pubID := setupHungPub(t, e.ctx, e.client, true)
	fcDone := make(chan error, 1)
	go func() {
		_, err := e.client.Publish(e.ctx, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("2")})
		fcDone <- err
	}()
	sHeld := e.startSubCtx(e.ctx, &proxypb.StartSubscriberCommand{Endpoint: addrIn, Subscription: "projects/p/subscriptions/s-held", AutoSettleMode: autoDrop})
	pushFakeMsg(fIn, "ack-held", "m-held", "held", false, 0)
	_ = sHeld.md()
	t0 := time.Now()
	_, err := e.client.ShutdownProxy(e.ctx, &proxypb.ShutdownProxyRequest{GracePeriodMs: 120})
	noErr(t, err, "ShutdownProxy")
	e.srv.waitClosed()
	req(t, time.Since(t0) <= time.Second, "ShutdownProxy(120ms) hung %v", time.Since(t0))
	<-fcDone
	binPath := filepath.Join(t.TempDir(), "go-conformance-proxy-testbin")
	t.Setenv("GOPROXY", "off")
	out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput()
	req(t, err == nil, "go build: %v\n%s", err, out)
	for _, useSIGTERM := range []bool{false, true} {
		bctx, bcancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(bctx, binPath, "--port=0")
		stdout, _ := cmd.StdoutPipe()
		noErr(t, cmd.Start(), "start")
		line, err := bufio.NewReader(stdout).ReadString('\n')
		req(t, err == nil && strings.HasPrefix(line, "READY 127.0.0.1:"), "READY = %q err=%v", line, err)
		conn, _ := grpc.NewClient("passthrough:///"+strings.TrimSpace(strings.TrimPrefix(line, "READY ")), grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := proxypb.NewPubSubProxyClient(conn)
		_, _, _ = setupHungPub(t, bctx, client, false)
		start := time.Now()
		if useSIGTERM {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		} else {
			_, _ = client.ShutdownProxy(bctx, &proxypb.ShutdownProxyRequest{GracePeriodMs: 0})
		}
		_ = conn.Close()
		_, _ = io.Copy(io.Discard, stdout)
		req(t, cmd.Wait() == nil && time.Since(start) <= map[bool]time.Duration{false: 2 * time.Second, true: 6500 * time.Millisecond}[useSIGTERM], "shutdown(%v)", useSIGTERM)
		bcancel()
	}
}
