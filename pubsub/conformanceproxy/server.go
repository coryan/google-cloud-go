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
	"errors"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"cloud.google.com/go/pubsub/v2"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const clientLibraryVersion = "2.9.0"

// emulatorEnvMu serializes PUBSUB_EMULATOR_HOST reads/writes around
// pubsub.NewClientWithConfig because pubsub/v2 inspects the process env var.
var emulatorEnvMu sync.Mutex

type proxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
	grpcSrv                 *grpc.Server
	traces                  *traceManager
	mu                      sync.Mutex
	nextPubID               atomic.Int64
	publishers              map[string]*publisherHandle
	subscribers             map[*subscriberSession]struct{}
	shutdownOnce, closeOnce sync.Once
	shuttingDown            atomic.Bool
	closedCh                chan struct{}
}

func newProxyServer(grpcSrv *grpc.Server, traces *traceManager) *proxyServer {
	return &proxyServer{
		grpcSrv: grpcSrv, traces: traces, publishers: make(map[string]*publisherHandle),
		subscribers: make(map[*subscriberSession]struct{}), closedCh: make(chan struct{}),
	}
}

func (s *proxyServer) GetCapabilities(_ context.Context, _ *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return &proxypb.GetCapabilitiesResponse{
		ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: clientLibraryVersion, SupportsMonitoring: true,
		SupportsUnaryPull: false, SupportsBlockingPublish: false, SupportsPublishFlowControl: true, SupportsPublishCompression: true,
		SupportsPublishHedging: true, SupportsOrderingOptIn: true, SupportsSubscriberOrdering: true, SupportsKeepalive: true,
		SupportsEodReceiptModack: true, SupportsAutoNackOnDrop: false, SupportsAckWithResult: true, SupportsCustomRetryCodes: true,
		SupportsExactlyOnceDelivery: true, SupportsPerStreamFlowControl: true, SupportsResumePublish: true, SupportsShutdownBehavior: true, SupportsShutdownTimeout: true,
		Defaults: &proxypb.ClientDefaults{
			PublishBatchCountThreshold: int64(pubsub.DefaultPublishSettings.CountThreshold), PublishBatchByteThreshold: int64(pubsub.DefaultPublishSettings.ByteThreshold),
			PublishMaxRequestBytes: int64(pubsub.MaxPublishRequestBytes), PublishBatchDelayMs: pubsub.DefaultPublishSettings.DelayThreshold.Milliseconds(),
			PublishTotalTimeoutMs: pubsub.DefaultPublishSettings.Timeout.Milliseconds(), PublishFlowControlMaxMessages: int64(pubsub.DefaultPublishSettings.FlowControlSettings.MaxOutstandingMessages),
			PublishFlowControlMaxBytes: int64(pubsub.DefaultPublishSettings.FlowControlSettings.MaxOutstandingBytes), PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE,
			PublishCompressionEnabledByDefault: pubsub.DefaultPublishSettings.EnableCompression, PublishCompressionBytesThreshold: int64(pubsub.DefaultPublishSettings.CompressionBytesThreshold),
			PublishCompressionAlgorithm: proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP, PublishHedgingDelayMs: 1000, PublishHedgingMaxTokens: 50, PublishHedgingRefillRatio: 0.1,
			SubscriberMaxOutstandingMessages: int64(pubsub.DefaultReceiveSettings.MaxOutstandingMessages), SubscriberMaxOutstandingBytes: int64(pubsub.DefaultReceiveSettings.MaxOutstandingBytes),
			SubscriberMaxExtensionMs: pubsub.DefaultReceiveSettings.MaxExtension.Milliseconds(), SubscriberMinAckExtensionMs: 10_000, SubscriberMaxAckExtensionMs: 600_000,
			SubscriberAdaptiveAckExtension: true, SubscriberPerStreamFlowControl: pubsub.DefaultReceiveSettings.EnablePerStreamFlowControl, SubscriberDefaultStreamAckDeadlineSeconds: 60,
			KeepalivePingIntervalMs: 30_000, KeepalivePongTimeoutMs: 15_000, RetryInitialDelayMs: 100, RetryMaxDelayMs: 60_000, RetryDelayMultiplier: 4.0,
		},
	}, nil
}

func (s *proxyServer) ResetTraces(ctx context.Context, _ *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	s.traces.Reset(ctx)
	return &proxypb.ResetTracesResponse{}, nil
}
func (s *proxyServer) DrainTraces(ctx context.Context, _ *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	return &proxypb.DrainTracesResponse{Spans: s.traces.Drain(ctx)}, nil
}
func (s *proxyServer) UnaryPull(_ context.Context, _ *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	return nil, status.Error(codes.Unimplemented, "pubsub/v2 high-level client does not expose unary Pull")
}

func (s *proxyServer) ShutdownProxy(_ context.Context, req *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	grace := 500 * time.Millisecond
	if req != nil && req.GetGracePeriodMs() > 0 {
		grace = clampMsToDuration(req.GetGracePeriodMs())
	}
	go s.initiateShutdown(grace)
	return &proxypb.ShutdownProxyResponse{}, nil
}

func (s *proxyServer) initiateShutdown(grace time.Duration) {
	s.shutdownOnce.Do(func() {
		s.shuttingDown.Store(true)
		defer s.closeOnce.Do(func() { close(s.closedCh) })
		if grace <= 0 {
			grace = 200 * time.Millisecond
		}
		deadline := time.NewTimer(grace)
		defer deadline.Stop()

		s.mu.Lock()
		pubs, subs := slices.Collect(maps.Values(s.publishers)), slices.Collect(maps.Keys(s.subscribers))
		clear(s.publishers)
		clear(s.subscribers)
		s.mu.Unlock()
		for _, sess := range subs {
			sess.abort()
		}

		pubDone := make(chan struct{})
		go func() {
			defer close(pubDone)
			var wg sync.WaitGroup
			for _, h := range pubs {
				wg.Go(h.close)
			}
			wg.Wait()
		}()

		var grpcDone chan struct{}
		grpcFinished := s.grpcSrv == nil
		if !grpcFinished {
			grpcDone = make(chan struct{})
			go func() { defer close(grpcDone); s.grpcSrv.GracefulStop() }()
		}
		for pubsFinished := false; !pubsFinished || !grpcFinished; {
			select {
			case <-pubDone:
				pubsFinished, pubDone = true, nil
			case <-grpcDone:
				grpcFinished, grpcDone = true, nil
			case <-deadline.C:
				for _, h := range pubs {
					h.closeClient()
				}
				if s.grpcSrv != nil {
					s.grpcSrv.Stop()
					stopDrain := time.NewTimer(100 * time.Millisecond)
					defer stopDrain.Stop()
					select {
					case <-grpcDone:
					case <-stopDrain.C:
					}
				}
				return
			}
		}
	})
}

func (s *proxyServer) waitClosed() { <-s.closedCh }

func newPubSubClient(ctx context.Context, endpoint, explicitProjectID, resource string, useEmulatorEnv bool,
	customHeaders map[string]string, cfg *pubsub.ClientConfig, extraOpts ...option.ClientOption) (*pubsub.Client, error) {
	emulatorEnvMu.Lock()
	defer emulatorEnvMu.Unlock()

	prevEmulator, hadEmulator := os.LookupEnv("PUBSUB_EMULATOR_HOST")
	defer func() {
		if hadEmulator {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", prevEmulator)
		} else {
			_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
		}
	}()

	// Always disable otelgrpc transport instrumentation while leaving
	// ClientConfig.EnableOpenTelemetryTracing controlled by enable_monitoring.
	opts := []option.ClientOption{option.WithTelemetryDisabled()}
	if useEmulatorEnv {
		if endpoint != "" {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", endpoint)
		}
	} else {
		_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
		opts = append(opts, option.WithEndpoint(endpoint), option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}
	if len(customHeaders) > 0 {
		kv := make([]string, 0, len(customHeaders)*2)
		for _, k := range slices.Sorted(maps.Keys(customHeaders)) {
			kv = append(kv, k, customHeaders[k])
		}
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				return invoker(metadata.AppendToOutgoingContext(ctx, kv...), method, req, reply, cc, callOpts...)
			})),
			option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.AppendToOutgoingContext(ctx, kv...), desc, cc, method, callOpts...)
			})),
		)
	}
	opts = append(opts, extraOpts...)
	projectID := explicitProjectID
	if projectID == "" {
		if parts := strings.Split(resource, "/"); len(parts) == 4 && parts[0] == "projects" && parts[1] != "" {
			projectID = parts[1]
		}
	}
	c, err := pubsub.NewClientWithConfig(ctx, projectID, cfg, opts...)
	if errors.Is(err, pubsub.ErrEmptyProjectID) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "create pubsub client: %v", err)
	}
	return c, nil
}

func validateRetryOverrides(initialMs, maxMs *int64, multiplier *float64) error {
	if initialMs != nil && *initialMs < 0 {
		return status.Error(codes.InvalidArgument, "initial_retry_delay_ms must be >= 0")
	}
	if maxMs != nil && *maxMs < 0 {
		return status.Error(codes.InvalidArgument, "max_retry_delay_ms must be >= 0")
	}
	if multiplier != nil && (math.IsNaN(*multiplier) || math.IsInf(*multiplier, 0) || *multiplier < 0) {
		return status.Error(codes.InvalidArgument, "retry_delay_multiplier must be finite and >= 0")
	}
	return nil
}

func setOpt[T any](dst, src *T) {
	if src != nil {
		*dst = *src
	}
}

func setInt[S ~int32 | ~int64](dst *int, src *S) {
	if src != nil {
		*dst = int(*src)
	}
}

func clampMsToDuration(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return clampSignedMsToDuration(ms)
}

func clampSignedMsToDuration(ms int64) time.Duration {
	const maxMs = math.MaxInt64 / int64(time.Millisecond)
	if ms > maxMs {
		return time.Duration(math.MaxInt64)
	}
	if ms < -maxMs {
		return time.Duration(math.MinInt64)
	}
	return time.Duration(ms) * time.Millisecond
}

type safeRetryer struct {
	codes       map[codes.Code]bool
	bo          gax.Backoff
	cur         time.Duration
	maxAttempts int32
	attempts    int32
}

func (r *safeRetryer) Retry(err error) (time.Duration, bool) {
	r.attempts++
	if r.maxAttempts > 0 && r.attempts >= r.maxAttempts {
		return 0, false
	}
	st, ok := status.FromError(err)
	if !ok || !r.codes[st.Code()] {
		return 0, false
	}
	if r.cur <= 0 {
		r.cur = r.bo.Initial
		if r.cur <= 0 {
			r.cur = time.Second
		}
	}
	maxD := r.bo.Max
	if maxD <= 0 {
		maxD = 30 * time.Second
	}
	d := time.Duration(1 + rand.Int64N(int64(r.cur)))
	next := float64(r.cur) * r.bo.Multiplier
	if math.IsNaN(next) || math.IsInf(next, 0) || next >= float64(maxD) {
		r.cur = maxD
	} else {
		r.cur = time.Duration(next)
	}
	return d, true
}

func buildRetryCodeSet(defaultCodes []codes.Code, rawCodes []int32) map[codes.Code]bool {
	retryCodes := make(map[codes.Code]bool, max(len(rawCodes), len(defaultCodes)))
	for _, c := range defaultCodes {
		retryCodes[c] = len(rawCodes) == 0
	}
	for _, c := range rawCodes {
		retryCodes[codes.Code(c)] = true
	}
	return retryCodes
}

func buildCustomRetryFactory(defaultCodes []codes.Code, defaultBackoff gax.Backoff, rawCodes []int32,
	initialMs, maxMs *int64, multiplier *float64, maxAttempts *int32) func() gax.Retryer {
	retryCodes := buildRetryCodeSet(defaultCodes, rawCodes)
	bo := defaultBackoff
	if initialMs != nil {
		bo.Initial = clampMsToDuration(*initialMs)
	}
	if maxMs != nil {
		bo.Max = clampMsToDuration(*maxMs)
	}
	setOpt(&bo.Multiplier, multiplier)
	if math.IsNaN(bo.Multiplier) || math.IsInf(bo.Multiplier, 0) || bo.Multiplier < 1.0 {
		bo.Multiplier = 1.0
	}
	var maxAtt int32
	setOpt(&maxAtt, maxAttempts)
	return func() gax.Retryer { return &safeRetryer{codes: retryCodes, bo: bo, maxAttempts: maxAtt} }
}

func customRetryCallOptions(defaultCodes []codes.Code, defaultBackoff gax.Backoff, rawCodes []int32,
	initialMs, maxMs *int64, multiplier *float64, maxAttempts *int32) []gax.CallOption {
	if len(rawCodes) == 0 && initialMs == nil && maxMs == nil && multiplier == nil && maxAttempts == nil {
		return nil
	}
	return []gax.CallOption{gax.WithRetry(buildCustomRetryFactory(defaultCodes, defaultBackoff, rawCodes, initialMs, maxMs, multiplier, maxAttempts))}
}

func errorToCodeAndMsg(err error) (int32, string) {
	if err == nil {
		return 0, ""
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		return int32(st.Code()), err.Error()
	}
	return int32(codes.Unknown), err.Error()
}
