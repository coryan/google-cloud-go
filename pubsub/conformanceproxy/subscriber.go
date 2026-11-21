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
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"cloud.google.com/go/pubsub/v2"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var defaultSubRetryCodes = []codes.Code{codes.Unavailable}
var defaultSubBackoff = gax.Backoff{Initial: 100 * time.Millisecond, Max: 60 * time.Second, Multiplier: 1.3}
var eodTempRetryCodes = map[codes.Code]bool{codes.DeadlineExceeded: true, codes.ResourceExhausted: true, codes.Aborted: true, codes.Internal: true, codes.Unavailable: true}

type pendingDelivery struct {
	settleCh chan proxypb.SettleMessageCommand_SettleAction
}

type eodFingerprint struct {
	msgID               string
	pubTimeNano         int64
	deliveryAttempt     int32
	orderingKey         string
	dataHash, attrsHash uint64
}

type eodEntry struct {
	ackID                            string
	fp                               eodFingerprint
	batchID, streamID                int64
	attempts                         int
	rem                              *int
	eod, ready, inFlight, superseded bool
}

type subscriberSession struct {
	stream                          grpc.BidiStreamingServer[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
	autoMode                        proxypb.StartSubscriberCommand_AutoSettleMode
	shutdownBehavior                proxypb.ShutdownBehavior
	recvCancel                      context.CancelFunc
	receiveExitedCh, sessionAbortCh chan struct{}
	abortOnce                       sync.Once
	clientMu, sendMu, pendingMu     sync.Mutex
	client                          *pubsub.Client
	stoppedSent, eodClosed          bool
	pending                         map[string]*pendingDelivery
	gaxRetryCodes                   map[codes.Code]bool
	minEODDeadlineSec               int32
	eodMu                           sync.Mutex
	eodByAckID                      map[string][]*eodEntry
	eodByFP                         map[eodFingerprint][]*eodEntry
	nextStreamID, nextBatchID       atomic.Int64
	nextToken, totalDelivered       atomic.Int64
	activeCallbacks                 atomic.Int32
}

func newSubscriberSession(stream grpc.BidiStreamingServer[proxypb.SubscriberCommand, proxypb.SubscriberEvent],
	cmd *proxypb.StartSubscriberCommand, recvCancel context.CancelFunc) *subscriberSession {
	minEOD := int32(60)
	if cmd.MinDurationPerAckExtensionMs != nil && *cmd.MinDurationPerAckExtensionMs > 0 {
		minEOD = int32(clampSignedMsToDuration(*cmd.MinDurationPerAckExtensionMs) / time.Second)
	}
	return &subscriberSession{
		stream: stream, autoMode: cmd.GetAutoSettleMode(), shutdownBehavior: cmd.GetShutdownBehavior(),
		recvCancel: recvCancel, receiveExitedCh: make(chan struct{}), sessionAbortCh: make(chan struct{}),
		pending: make(map[string]*pendingDelivery), gaxRetryCodes: buildRetryCodeSet(defaultSubRetryCodes, cmd.GetRetryCodes()),
		minEODDeadlineSec: minEOD, eodByAckID: make(map[string][]*eodEntry), eodByFP: make(map[eodFingerprint][]*eodEntry),
	}
}

func (sess *subscriberSession) abort() {
	sess.abortOnce.Do(func() {
		sess.recvCancel()
		sess.clientMu.Lock()
		c := sess.client
		sess.clientMu.Unlock()
		if c != nil {
			_ = c.Close()
		}
		close(sess.sessionAbortCh)
	})
}

func (sess *subscriberSession) sendLiveEvent(ev *proxypb.SubscriberEvent) error {
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	if sess.stoppedSent {
		return status.Error(codes.Canceled, "subscriber session already stopped")
	}
	select {
	case <-sess.receiveExitedCh:
		return status.Error(codes.Canceled, "subscriber receive exited")
	case <-sess.sessionAbortCh:
		return status.Error(codes.Canceled, "subscriber session aborted")
	default:
	}
	if err := sess.stream.Send(ev); err != nil {
		return err
	}
	if ev.GetMessageDelivered() != nil {
		sess.totalDelivered.Add(1)
	}
	return nil
}

func (sess *subscriberSession) sendStopped(code int32, errMsg string) error {
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	if sess.stoppedSent {
		return nil
	}
	sess.stoppedSent = true
	return sess.stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{
		Stopped: &proxypb.StoppedEvent{ErrorCode: code, ErrorMessage: errMsg, TotalDelivered: sess.totalDelivered.Load()},
	}})
}

func (sess *subscriberSession) popPending(token string) (*pendingDelivery, bool) {
	sess.pendingMu.Lock()
	pd, ok := sess.pending[token]
	delete(sess.pending, token)
	sess.pendingMu.Unlock()
	return pd, ok
}

func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func hashAttrs(attrs map[string]string) uint64 {
	if len(attrs) == 0 {
		return 0
	}
	h := fnv.New64a()
	var lenBuf [8]byte
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		for _, s := range []string{k, attrs[k]} {
			binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
			_, _ = h.Write(lenBuf[:])
			_, _ = h.Write([]byte(s))
		}
	}
	return h.Sum64()
}

func fingerprintFromProto(rm *pubsubpb.ReceivedMessage) eodFingerprint {
	msg := rm.GetMessage()
	pubTimeNano := (time.Time{}).UnixNano()
	if msg != nil {
		pubTimeNano = msg.GetPublishTime().AsTime().UnixNano()
	}
	da := max(rm.GetDeliveryAttempt(), 0)
	return eodFingerprint{
		msgID: msg.GetMessageId(), pubTimeNano: pubTimeNano, deliveryAttempt: da,
		orderingKey: msg.GetOrderingKey(), dataHash: hashBytes(msg.GetData()), attrsHash: hashAttrs(msg.GetAttributes()),
	}
}

func fingerprintFromMessage(m *pubsub.Message) eodFingerprint {
	var da int32
	if m.DeliveryAttempt != nil && *m.DeliveryAttempt > 0 {
		da = int32(*m.DeliveryAttempt)
	}
	return eodFingerprint{
		msgID: m.ID, pubTimeNano: m.PublishTime.UnixNano(), deliveryAttempt: da,
		orderingKey: m.OrderingKey, dataHash: hashBytes(m.Data), attrsHash: hashAttrs(m.Attributes),
	}
}

func unlinkMap[K comparable](m map[K][]*eodEntry, k K, target *eodEntry) bool {
	for i, item := range m[k] {
		if item == target {
			if q := slices.Delete(m[k], i, i+1); len(q) == 0 {
				delete(m, k)
			} else {
				m[k] = q
			}
			return true
		}
	}
	return false
}

func (sess *subscriberSession) unlinkAckIDLocked(e *eodEntry) {
	if unlinkMap(sess.eodByAckID, e.ackID, e) && e.rem != nil &&
		!slices.ContainsFunc(sess.eodByAckID[e.ackID], func(x *eodEntry) bool { return x.batchID == e.batchID }) {
		*e.rem--
	}
}

func (sess *subscriberSession) unlinkEODEntryLocked(e *eodEntry) {
	if !e.ready {
		sess.unlinkAckIDLocked(e)
		return
	}
	unlinkMap(sess.eodByFP, e.fp, e)
}

func (sess *subscriberSession) purgeStreamEODLocked(streamID int64) {
	if streamID > 0 {
		var stale []*eodEntry
		for _, q := range sess.eodByAckID {
			for _, e := range q {
				if e.streamID == streamID {
					e.superseded = true
					stale = append(stale, e)
				}
			}
		}
		for _, e := range stale {
			sess.unlinkEODEntryLocked(e)
		}
	}
}

func (sess *subscriberSession) recordReceivedEOD(streamID int64, rms []*pubsubpb.ReceivedMessage, eod bool) {
	sess.eodMu.Lock()
	defer sess.eodMu.Unlock()
	sess.purgeStreamEODLocked(streamID)
	if !eod {
		for _, rm := range rms {
			entry := &eodEntry{ackID: rm.GetAckId(), fp: fingerprintFromProto(rm), ready: true}
			sess.eodByFP[entry.fp] = append(sess.eodByFP[entry.fp], entry)
		}
		return
	}
	batchID, seen := sess.nextBatchID.Add(1), make(map[string]struct{}, len(rms))
	for _, rm := range rms {
		seen[rm.GetAckId()] = struct{}{}
	}
	rem := len(seen)
	for _, rm := range rms {
		aid := rm.GetAckId()
		entry := &eodEntry{ackID: aid, fp: fingerprintFromProto(rm), batchID: batchID, streamID: streamID, rem: &rem, eod: true}
		sess.eodByAckID[aid] = append(sess.eodByAckID[aid], entry)
	}
}

func (sess *subscriberSession) popReceivedEOD(m *pubsub.Message) bool {
	fp := fingerprintFromMessage(m)
	sess.eodMu.Lock()
	defer sess.eodMu.Unlock()
	if q := sess.eodByFP[fp]; len(q) > 0 {
		eod := q[0].eod
		sess.unlinkEODEntryLocked(q[0])
		return eod
	}
	return false
}

func (sess *subscriberSession) claimModAckBatch(req *pubsubpb.ModifyAckDeadlineRequest) []*eodEntry {
	if req == nil || len(req.GetAckIds()) == 0 || req.GetAckDeadlineSeconds() <= 0 || req.GetAckDeadlineSeconds() < sess.minEODDeadlineSec {
		return nil
	}
	reqIDs := req.GetAckIds()
	sess.eodMu.Lock()
	defer sess.eodMu.Unlock()
	first := sess.eodByAckID[reqIDs[0]]
	if len(first) == 0 {
		return nil
	}
	var bestBatchID int64
	bestAttempts, bestExact := 0, false
	var bestClaimed []*eodEntry
	seenBatch := make(map[int64]bool, len(first))
candLoop:
	for _, cand := range first {
		if cand.inFlight || cand.superseded || seenBatch[cand.batchID] {
			continue
		}
		seenBatch[cand.batchID] = true
		var claimed []*eodEntry
		attempts := 0
		for _, aid := range reqIDs {
			before := len(claimed)
			for _, e := range sess.eodByAckID[aid] {
				if e.batchID == cand.batchID && !e.ready && !e.inFlight && !e.superseded {
					claimed = append(claimed, e)
					attempts = max(attempts, e.attempts)
				}
			}
			if len(claimed) == before {
				continue candLoop
			}
		}
		exact := cand.rem != nil && *cand.rem == len(reqIDs)
		if len(bestClaimed) == 0 || (exact && !bestExact) ||
			(exact == bestExact && (attempts < bestAttempts || (attempts == bestAttempts && cand.batchID < bestBatchID))) {
			bestBatchID, bestAttempts, bestExact, bestClaimed = cand.batchID, attempts, exact, claimed
		}
	}
	for _, e := range bestClaimed {
		e.inFlight = true
		e.attempts++
	}
	return bestClaimed
}

func (sess *subscriberSession) finishModAckBatch(claimed []*eodEntry, err error) {
	if len(claimed) == 0 {
		return
	}
	if err == nil {
		sess.eodMu.Lock()
		for _, e := range claimed {
			e.inFlight = false
			sess.unlinkAckIDLocked(e)
			if !e.superseded && !e.ready && !sess.eodClosed {
				e.ready = true
				sess.eodByFP[e.fp] = append(sess.eodByFP[e.fp], e)
			}
		}
		sess.eodMu.Unlock()
		return
	}
	st, ok := status.FromError(err)
	var errInfoMeta map[string]string
	if ok {
		for _, d := range st.Details() {
			if ei, isEI := d.(*errdetails.ErrorInfo); isEI && len(ei.GetMetadata()) > 0 {
				errInfoMeta = ei.GetMetadata()
				break
			}
		}
	}
	gaxRetry, eodTemp := ok && sess.gaxRetryCodes[st.Code()], ok && eodTempRetryCodes[st.Code()]
	sess.eodMu.Lock()
	for _, e := range claimed {
		e.inFlight = false
		if val, hasMeta := errInfoMeta[e.ackID]; !gaxRetry && ((hasMeta && !strings.HasPrefix(val, "TRANSIENT_")) || (!hasMeta && !eodTemp)) {
			sess.unlinkEODEntryLocked(e)
		}
	}
	sess.eodMu.Unlock()
}

type eodStreamWrapper struct {
	grpc.ClientStream
	sess     *subscriberSession
	streamID int64
}

func (w *eodStreamWrapper) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)
	if res, ok := m.(*pubsubpb.StreamingPullResponse); err == nil && ok && len(res.GetReceivedMessages()) > 0 {
		w.sess.recordReceivedEOD(w.streamID, res.GetReceivedMessages(), res.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled())
		return nil
	}
	w.sess.eodMu.Lock()
	w.sess.purgeStreamEODLocked(w.streamID)
	w.sess.eodMu.Unlock()
	return err
}

func (sess *subscriberSession) unaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		modReq, _ := req.(*pubsubpb.ModifyAckDeadlineRequest)
		if !strings.HasSuffix(method, "/ModifyAckDeadline") {
			modReq = nil
		}
		claimed := sess.claimModAckBatch(modReq)
		err := invoker(ctx, method, req, reply, cc, opts...)
		sess.finishModAckBatch(claimed, err)
		return err
	}
}

func (sess *subscriberSession) streamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(method, "/StreamingPull") {
			return &eodStreamWrapper{ClientStream: cs, sess: sess, streamID: sess.nextStreamID.Add(1)}, nil
		}
		return cs, nil
	}
}

func (s *proxyServer) SubscriberSession(stream grpc.BidiStreamingServer[proxypb.SubscriberCommand, proxypb.SubscriberEvent]) error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "proxy is shutting down")
	}
	firstCmd, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "stream closed before StartSubscriberCommand")
		}
		return err
	}
	startCmd := firstCmd.GetStart()
	if startCmd == nil {
		return status.Error(codes.InvalidArgument, "first command in SubscriberSession must be StartSubscriberCommand")
	}
	if err := validateRetryOverrides(startCmd.InitialRetryDelayMs, startCmd.MaxRetryDelayMs, startCmd.RetryDelayMultiplier); err != nil {
		return err
	}

	recvCtx, recvCancel := context.WithCancel(context.Background())
	sess := newSubscriberSession(stream, startCmd, recvCancel)
	defer func() {
		sess.sendMu.Lock()
		sess.stoppedSent = true
		sess.sendMu.Unlock()
		sess.abort()
	}()

	cfg := &pubsub.ClientConfig{EnableOpenTelemetryTracing: startCmd.GetEnableMonitoring()}
	if co := customRetryCallOptions(defaultSubRetryCodes, defaultSubBackoff, startCmd.GetRetryCodes(),
		startCmd.InitialRetryDelayMs, startCmd.MaxRetryDelayMs, startCmd.RetryDelayMultiplier, nil); len(co) > 0 {
		cfg.SubscriptionAdminCallOptions = &vkit.SubscriptionAdminCallOptions{Acknowledge: co, ModifyAckDeadline: co, StreamingPull: co, Pull: co}
	}
	client, err := newPubSubClient(stream.Context(), startCmd.GetEndpoint(), startCmd.GetProjectId(),
		startCmd.GetSubscription(), startCmd.GetUseEmulatorEnv(), startCmd.GetCustomHeaders(), cfg,
		option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(sess.unaryInterceptor())),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(sess.streamInterceptor())))
	if err != nil {
		return sess.sendStopped(errorToCodeAndMsg(err))
	}
	sess.clientMu.Lock()
	sess.client = client
	sess.clientMu.Unlock()

	s.mu.Lock()
	if s.shuttingDown.Load() {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "proxy is shutting down")
	}
	s.subscribers[sess] = struct{}{}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.subscribers, sess); s.mu.Unlock() }()

	sub := client.Subscriber(startCmd.GetSubscription())
	applySubscriberSettings(sub, startCmd)
	sess.sendMu.Lock()
	err = sess.stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{}}})
	sess.sendMu.Unlock()
	if err != nil {
		return err
	}

	// Watch gRPC stream context cancellation independently from StopSubscriberCommand.
	// Closing stream send side (EOF) must NOT abort callbacks waiting for StopSubscriber.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-stream.Context().Done():
			sess.abort()
		case <-watchDone:
		}
	}()
	go sess.commandLoop()

	recvErr := sub.Receive(recvCtx, sess.handleMessage)
	close(sess.receiveExitedCh)
	sess.eodMu.Lock()
	sess.eodClosed = true
	clear(sess.eodByAckID)
	clear(sess.eodByFP)
	sess.eodMu.Unlock()
	return sess.sendStopped(errorToCodeAndMsg(recvErr))
}

func applySubscriberSettings(sub *pubsub.Subscriber, cmd *proxypb.StartSubscriberCommand) {
	setInt(&sub.ReceiveSettings.MaxOutstandingMessages, cmd.MaxOutstandingMessages)
	setInt(&sub.ReceiveSettings.MaxOutstandingBytes, cmd.MaxOutstandingBytes)
	setOpt(&sub.ReceiveSettings.EnablePerStreamFlowControl, cmd.EnablePerStreamFlowControl)
	setInt(&sub.ReceiveSettings.NumGoroutines, cmd.NumStreams)
	if cmd.MaxExtensionMs != nil {
		sub.ReceiveSettings.MaxExtension = cmp.Or(clampMsToDuration(*cmd.MaxExtensionMs), -1)
	}
	// Forward Min/MaxDurationPerAckExtension raw to surface real pubsub/v2 behavior.
	if cmd.MinDurationPerAckExtensionMs != nil {
		sub.ReceiveSettings.MinDurationPerAckExtension = clampSignedMsToDuration(*cmd.MinDurationPerAckExtensionMs)
	}
	if cmd.MaxDurationPerAckExtensionMs != nil {
		sub.ReceiveSettings.MaxDurationPerAckExtension = clampSignedMsToDuration(*cmd.MaxDurationPerAckExtensionMs)
	}
	if cmd.GetShutdownBehavior() != proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_UNSPECIFIED || cmd.ShutdownTimeoutMs != nil {
		so := &pubsub.ShutdownOptions{Behavior: pubsub.ShutdownBehaviorWaitForProcessing, Timeout: -1}
		if cmd.GetShutdownBehavior() == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY {
			so.Behavior, so.Timeout = pubsub.ShutdownBehaviorNackImmediately, 10*time.Second
			if cmd.ShutdownTimeoutMs != nil && *cmd.ShutdownTimeoutMs >= 0 {
				so.Timeout = clampMsToDuration(*cmd.ShutdownTimeoutMs)
			}
		} else if cmd.ShutdownTimeoutMs != nil {
			so.Timeout = clampSignedMsToDuration(*cmd.ShutdownTimeoutMs)
		}
		sub.ReceiveSettings.ShutdownOptions = so
	}
}

func (sess *subscriberSession) commandLoop() {
	for {
		cmd, err := sess.stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				sess.abort()
			}
			return
		}
		switch c := cmd.GetCommand().(type) {
		case *proxypb.SubscriberCommand_Settle:
			if c.Settle != nil {
				if pd, ok := sess.popPending(c.Settle.GetDeliveryToken()); ok {
					select {
					case pd.settleCh <- c.Settle.GetAction():
					default:
					}
				}
			}
		case *proxypb.SubscriberCommand_Stop:
			sess.recvCancel()
		}
	}
}

func (sess *subscriberSession) handleMessage(cbCtx context.Context, m *pubsub.Message) {
	active := sess.activeCallbacks.Add(1)
	defer sess.activeCallbacks.Add(-1)
	token := fmt.Sprintf("del-%d", sess.nextToken.Add(1))
	cbTP, cbTS := injectCallbackTraceContext(cbCtx)
	isEOD := sess.popReceivedEOD(m)
	var deliveryAttempt int32
	var hasDeliveryAttempt bool
	if m.DeliveryAttempt != nil {
		hasDeliveryAttempt, deliveryAttempt = true, int32(*m.DeliveryAttempt)
	}
	autoSettled := sess.autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_UNSPECIFIED &&
		sess.autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE
	var pd *pendingDelivery
	if !autoSettled {
		pd = &pendingDelivery{settleCh: make(chan proxypb.SettleMessageCommand_SettleAction, 1)}
		sess.pendingMu.Lock()
		sess.pending[token] = pd
		sess.pendingMu.Unlock()
	}
	ev := &proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{
		DeliveryToken: token, MessageId: m.ID, Data: m.Data, Attributes: m.Attributes, OrderingKey: m.OrderingKey,
		PublishTime: timestamppb.New(m.PublishTime), DeliveryAttempt: deliveryAttempt, HasDeliveryAttempt: hasDeliveryAttempt,
		ActiveCallbacks: active, CallbackTraceparent: cbTP, CallbackTracestate: cbTS, IsExactlyOnce: isEOD, AutoSettled: autoSettled,
	}}}
	if err := sess.sendLiveEvent(ev); err != nil {
		if !autoSettled {
			sess.popPending(token)
		}
		select {
		case <-sess.receiveExitedCh:
		default:
			sess.abort()
			if !autoSettled || sess.autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE {
				m.Nack()
			}
		}
		return
	}
	switch sess.autoMode {
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK:
		m.Ack()
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK:
		m.Nack()
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE:
		return
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT:
		sess.settleAndEmitResult(token, m.AckWithResult(), false)
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT:
		sess.settleAndEmitResult(token, m.NackWithResult(), true)
	default:
		// INTERACTIVE: honor cbCtx.Done() only under NACK_IMMEDIATELY (pubsub/v2
		// cancels cbCtx immediately on Stop even under WaitForProcessing).
		var cbDone <-chan struct{}
		if sess.shutdownBehavior == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY {
			cbDone = cbCtx.Done()
		}
		select {
		case action := <-pd.settleCh:
			switch action {
			case proxypb.SettleMessageCommand_SETTLE_ACTION_ACK:
				m.Ack()
			case proxypb.SettleMessageCommand_SETTLE_ACTION_NACK:
				m.Nack()
			case proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT:
				sess.settleAndEmitResult(token, m.AckWithResult(), false)
			case proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT:
				sess.settleAndEmitResult(token, m.NackWithResult(), true)
			case proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE:
				return
			}
		case <-cbDone:
			sess.popPending(token)
		case <-sess.receiveExitedCh:
			sess.popPending(token)
		case <-sess.sessionAbortCh:
			sess.popPending(token)
			select {
			case <-sess.receiveExitedCh:
			default:
				m.Nack()
			}
		}
	}
}

func (sess *subscriberSession) settleAndEmitResult(token string, ar *pubsub.AckResult, isNack bool) {
	waitCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer func() { cancel(); close(done) }()
	go func() {
		select {
		case <-sess.receiveExitedCh:
			cancel()
		case <-sess.sessionAbortCh:
			cancel()
		case <-done:
		}
	}()
	ackStatus, err := ar.Get(waitCtx)
	if waitCtx.Err() != nil {
		return
	}
	grpcCode, errMsg := errorToCodeAndMsg(err)
	if grpcCode == int32(codes.Unknown) && ackStatus == pubsub.AcknowledgeStatusInvalidAckID {
		grpcCode = int32(codes.InvalidArgument)
	}
	_ = sess.sendLiveEvent(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{
		DeliveryToken: token, Status: mapAckStatus(ackStatus), GrpcErrorCode: grpcCode, ErrorMessage: errMsg, IsNack: isNack,
	}}})
}

var ackStatusMap = map[pubsub.AcknowledgeStatus]proxypb.SettlementResultEvent_AckStatus{
	pubsub.AcknowledgeStatusSuccess: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, pubsub.AcknowledgeStatusPermissionDenied: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED,
	pubsub.AcknowledgeStatusFailedPrecondition: proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, pubsub.AcknowledgeStatusInvalidAckID: proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID,
	pubsub.AcknowledgeStatusOther: proxypb.SettlementResultEvent_ACK_STATUS_OTHER,
}

func mapAckStatus(st pubsub.AcknowledgeStatus) proxypb.SettlementResultEvent_AckStatus {
	return cmp.Or(ackStatusMap[st], proxypb.SettlementResultEvent_ACK_STATUS_UNKNOWN)
}
