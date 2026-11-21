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
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"cloud.google.com/go/pubsub/v2"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var defaultPublishRetryCodes = []codes.Code{
	codes.Aborted, codes.Canceled, codes.Internal, codes.ResourceExhausted, codes.Unknown, codes.Unavailable, codes.DeadlineExceeded,
}
var defaultPublishBackoff = gax.Backoff{Initial: 100 * time.Millisecond, Max: 60 * time.Second, Multiplier: 4.0}

type pendingPublish struct {
	res                      *pubsub.PublishResult
	verifyImmut              bool
	passedAttrs, beforeAttrs map[string]string
}

type publisherHandle struct {
	client                    *pubsub.Client
	pub                       *pubsub.Publisher
	mu                        sync.Mutex
	nextToken                 atomic.Int64
	pending                   map[string]*pendingPublish
	stopOnce, closeClientOnce sync.Once
}

func (h *publisherHandle) stop()        { h.stopOnce.Do(func() { h.pub.Stop() }) }
func (h *publisherHandle) closeClient() { h.closeClientOnce.Do(func() { _ = h.client.Close() }) }
func (h *publisherHandle) close()       { h.stop(); h.closeClient() }

func (s *proxyServer) getPublisher(id string) (*publisherHandle, error) {
	s.mu.Lock()
	h, ok := s.publishers[id]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publisher_id %q", id)
	}
	return h, nil
}

func (s *proxyServer) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	if s.shuttingDown.Load() {
		return nil, status.Error(codes.Unavailable, "proxy is shutting down")
	}
	if req.GetCompressionAlgorithm() == proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE {
		return nil, status.Error(codes.Unimplemented, "pubsub/v2 only supports gzip publish compression")
	}
	if req.NumWorkers != nil && *req.NumWorkers < 0 {
		return nil, status.Error(codes.InvalidArgument, "num_workers must be >= 0")
	}
	if err := validateRetryOverrides(req.InitialRetryDelayMs, req.MaxRetryDelayMs, req.RetryDelayMultiplier); err != nil {
		return nil, err
	}

	cfg := &pubsub.ClientConfig{EnableOpenTelemetryTracing: req.GetEnableMonitoring()}
	retryOpts := customRetryCallOptions(defaultPublishRetryCodes, defaultPublishBackoff,
		req.GetRetryCodes(), req.InitialRetryDelayMs, req.MaxRetryDelayMs, req.RetryDelayMultiplier, req.MaxAttempts)
	var pubCallOpts []gax.CallOption
	if req.TotalTimeoutMs != nil && (*req.TotalTimeoutMs > 60_000 || (len(retryOpts) > 0 && *req.TotalTimeoutMs > 0)) {
		pubCallOpts = append(pubCallOpts, gax.WithTimeout(clampMsToDuration(*req.TotalTimeoutMs)))
	}
	pubCallOpts = append(pubCallOpts, retryOpts...)
	if len(pubCallOpts) > 0 {
		cfg.TopicAdminCallOptions = &vkit.TopicAdminCallOptions{Publish: pubCallOpts}
	}

	client, err := newPubSubClient(ctx, req.GetEndpoint(), req.GetProjectId(), req.GetTopic(), req.GetUseEmulatorEnv(), req.GetCustomHeaders(), cfg)
	if err != nil {
		return nil, err
	}
	pub := client.Publisher(req.GetTopic())
	applyPublisherSettings(pub, req)
	id, h := fmt.Sprintf("pub-%d", s.nextPubID.Add(1)), &publisherHandle{client: client, pub: pub, pending: make(map[string]*pendingPublish)}

	s.mu.Lock()
	if s.shuttingDown.Load() {
		s.mu.Unlock()
		h.close()
		return nil, status.Error(codes.Unavailable, "proxy is shutting down")
	}
	s.publishers[id] = h
	s.mu.Unlock()

	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func applyPublisherSettings(pub *pubsub.Publisher, req *proxypb.CreatePublisherRequest) {
	setOpt(&pub.EnableMessageOrdering, req.EnableMessageOrdering)
	setInt(&pub.PublishSettings.CountThreshold, req.CountThreshold)
	setInt(&pub.PublishSettings.ByteThreshold, req.ByteThreshold)
	if req.DelayThresholdMs != nil {
		pub.PublishSettings.DelayThreshold = clampSignedMsToDuration(*req.DelayThresholdMs)
	}
	setInt(&pub.PublishSettings.NumGoroutines, req.NumWorkers)
	setInt(&pub.PublishSettings.FlowControlSettings.MaxOutstandingMessages, req.MaxOutstandingMessages)
	setInt(&pub.PublishSettings.FlowControlSettings.MaxOutstandingBytes, req.MaxOutstandingBytes)
	switch req.GetLimitExceededBehavior() {
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE:
		pub.PublishSettings.FlowControlSettings.LimitExceededBehavior = pubsub.FlowControlIgnore
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK:
		pub.PublishSettings.FlowControlSettings.LimitExceededBehavior = pubsub.FlowControlBlock
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR:
		pub.PublishSettings.FlowControlSettings.LimitExceededBehavior = pubsub.FlowControlSignalError
	}
	setOpt(&pub.PublishSettings.EnableCompression, req.EnableCompression)
	setInt(&pub.PublishSettings.CompressionBytesThreshold, req.CompressionBytesThreshold)
	if hdg := req.GetHedging(); hdg != nil && hdg.GetEnabled() {
		hs := &pubsub.HedgingSettings{}
		if hdg.DelayMs != nil {
			hs.Delay = clampSignedMsToDuration(*hdg.DelayMs)
		}
		setOpt(&hs.MaxTokens, hdg.MaxTokens)
		setOpt(&hs.RefillRatio, hdg.RefillRatio)
		pub.PublishSettings.HedgingSettings = hs
	}
	if req.TotalTimeoutMs != nil {
		pub.PublishSettings.Timeout = clampSignedMsToDuration(*req.TotalTimeoutMs)
	}
}

func (s *proxyServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	if req.GetUseBlockingPublisher() {
		return nil, status.Error(codes.Unimplemented, "pubsub/v2 does not support BlockingPublisher")
	}
	h, err := s.getPublisher(req.GetPublisherId())
	if err != nil {
		return nil, err
	}
	var passedAttrs, beforeAttrs map[string]string
	if len(req.GetAttributes()) > 0 || len(req.GetRawAttributes()) > 0 {
		passedAttrs = make(map[string]string, len(req.GetAttributes())+len(req.GetRawAttributes()))
		maps.Copy(passedAttrs, req.GetAttributes())
		for _, ra := range req.GetRawAttributes() {
			passedAttrs[string(ra.GetKey())] = string(ra.GetValue())
		}
	}
	verifyImmut := req.GetVerifyAttributesImmutability()
	if verifyImmut {
		if passedAttrs == nil {
			passedAttrs = make(map[string]string)
		}
		beforeAttrs = maps.Clone(passedAttrs)
	}
	msg := &pubsub.Message{Data: req.GetData(), Attributes: passedAttrs, OrderingKey: req.GetOrderingKey()}
	res := h.pub.Publish(extractRemoteTraceContext(ctx, req.GetParentTraceparent(), req.GetParentTracestate()), msg)
	if req.GetAsyncFire() {
		token := fmt.Sprintf("pubtok-%d", h.nextToken.Add(1))
		h.mu.Lock()
		h.pending[token] = &pendingPublish{res: res, verifyImmut: verifyImmut, passedAttrs: passedAttrs, beforeAttrs: beforeAttrs}
		h.mu.Unlock()
		return &proxypb.PublishResponse{PublishToken: token, CallerAttributesMutated: verifyImmut && !maps.Equal(passedAttrs, beforeAttrs)}, nil
	}
	return awaitPublishResult(ctx, res, verifyImmut, passedAttrs, beforeAttrs), nil
}

func awaitPublishResult(ctx context.Context, res *pubsub.PublishResult, verify bool, passed, before map[string]string) *proxypb.PublishResponse {
	msgID, pubErr := res.Get(ctx)
	code, errMsg := errorToCodeAndMsg(pubErr)
	return &proxypb.PublishResponse{
		MessageId: msgID, ErrorCode: code, ErrorMessage: errMsg, CallerAttributesMutated: verify && !maps.Equal(passed, before),
	}
}

func (s *proxyServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	h, err := s.getPublisher(req.GetPublisherId())
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	entry, ok := h.pending[req.GetPublishToken()]
	delete(h.pending, req.GetPublishToken())
	h.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publish_token %q", req.GetPublishToken())
	}
	return awaitPublishResult(ctx, entry.res, entry.verifyImmut, entry.passedAttrs, entry.beforeAttrs), nil
}

func doPub[Resp any](s *proxyServer, id string, fn func(*publisherHandle)) (*Resp, error) {
	h, err := s.getPublisher(id)
	if err != nil {
		return nil, err
	}
	fn(h)
	return new(Resp), nil
}

func (s *proxyServer) FlushPublisher(_ context.Context, req *proxypb.FlushPublisherRequest) (*proxypb.FlushPublisherResponse, error) {
	return doPub[proxypb.FlushPublisherResponse](s, req.GetPublisherId(), func(h *publisherHandle) { h.pub.Flush() })
}

func (s *proxyServer) StopPublisher(_ context.Context, req *proxypb.StopPublisherRequest) (*proxypb.StopPublisherResponse, error) {
	return doPub[proxypb.StopPublisherResponse](s, req.GetPublisherId(), func(h *publisherHandle) { h.pub.Stop() })
}

func (s *proxyServer) ResumePublish(_ context.Context, req *proxypb.ResumePublishRequest) (*proxypb.ResumePublishResponse, error) {
	return doPub[proxypb.ResumePublishResponse](s, req.GetPublisherId(), func(h *publisherHandle) { h.pub.ResumePublish(req.GetOrderingKey()) })
}

func (s *proxyServer) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	s.mu.Lock()
	h, ok := s.publishers[req.GetPublisherId()]
	delete(s.publishers, req.GetPublisherId())
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publisher_id %q", req.GetPublisherId())
	}
	h.close()
	return &proxypb.ClosePublisherResponse{}, nil
}
