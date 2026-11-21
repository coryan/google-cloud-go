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
	"sync"

	"cloud.google.com/go/pubsub/conformanceproxy/proxypb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	otrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type traceStore struct {
	mu    sync.Mutex
	spans []*proxypb.TraceSpan
}

func newTraceStore() *traceStore {
	return &traceStore{}
}

func (ts *traceStore) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	converted := make([]*proxypb.TraceSpan, 0, len(spans))
	for _, s := range spans {
		converted = append(converted, convertSpan(s))
	}
	ts.mu.Lock()
	ts.spans = append(ts.spans, converted...)
	ts.mu.Unlock()
	return nil
}

func (ts *traceStore) Shutdown(_ context.Context) error {
	return nil
}

func (ts *traceStore) reset() {
	ts.mu.Lock()
	ts.spans = nil
	ts.mu.Unlock()
}

func (ts *traceStore) drain() []*proxypb.TraceSpan {
	ts.mu.Lock()
	out := ts.spans
	ts.spans = nil
	ts.mu.Unlock()
	return out
}

type traceManager struct {
	tp    *sdktrace.TracerProvider
	store *traceStore
}

func initTracing() *traceManager {
	store := newTraceStore()
	// WithRawSpanLimits preserves -1 (unlimited) whereas deprecated WithSpanLimits
	// overwrites <= 0 with default 128, breaking 1000-message batch link assertions.
	limits := sdktrace.NewSpanLimits()
	limits.AttributeCountLimit = -1
	limits.AttributeValueLengthLimit = -1
	limits.EventCountLimit = -1
	limits.LinkCountLimit = -1
	limits.AttributePerEventCountLimit = -1
	limits.AttributePerLinkCountLimit = -1

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(store),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithRawSpanLimits(limits),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return &traceManager{tp: tp, store: store}
}

func (tm *traceManager) Reset(ctx context.Context) {
	if tm == nil {
		return
	}
	_ = tm.tp.ForceFlush(ctx)
	tm.store.reset()
}

func (tm *traceManager) Drain(ctx context.Context) []*proxypb.TraceSpan {
	if tm == nil {
		return nil
	}
	_ = tm.tp.ForceFlush(ctx)
	return tm.store.drain()
}

func (tm *traceManager) Shutdown(ctx context.Context) error {
	if tm == nil {
		return nil
	}
	return tm.tp.Shutdown(ctx)
}

func extractRemoteTraceContext(ctx context.Context, traceparent, tracestate string) context.Context {
	if traceparent == "" && tracestate == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	if traceparent != "" {
		carrier["traceparent"] = traceparent
	}
	if tracestate != "" {
		carrier["tracestate"] = tracestate
	}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}

func injectCallbackTraceContext(ctx context.Context) (string, string) {
	if !otrace.SpanContextFromContext(ctx).IsValid() {
		return "", ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"], carrier["tracestate"]
}

func convertSpan(s sdktrace.ReadOnlySpan) *proxypb.TraceSpan {
	sc := s.SpanContext()
	var traceID, spanID string
	if sc.TraceID().IsValid() {
		traceID = sc.TraceID().String()
	}
	if sc.SpanID().IsValid() {
		spanID = sc.SpanID().String()
	}
	var parentSpanID string
	if p := s.Parent(); p.IsValid() {
		parentSpanID = p.SpanID().String()
	}

	events := make([]*proxypb.SpanEvent, 0, len(s.Events()))
	for _, ev := range s.Events() {
		events = append(events, &proxypb.SpanEvent{
			Name:                   ev.Name,
			Timestamp:              timestamppb.New(ev.Time),
			Attributes:             convertAttributes(ev.Attributes),
			DroppedAttributesCount: uint32(ev.DroppedAttributeCount),
		})
	}

	links := make([]*proxypb.SpanLink, 0, len(s.Links()))
	for _, lk := range s.Links() {
		var lTraceID, lSpanID string
		if lk.SpanContext.TraceID().IsValid() {
			lTraceID = lk.SpanContext.TraceID().String()
		}
		if lk.SpanContext.SpanID().IsValid() {
			lSpanID = lk.SpanContext.SpanID().String()
		}
		links = append(links, &proxypb.SpanLink{
			TraceId:                lTraceID,
			SpanId:                 lSpanID,
			TraceState:             lk.SpanContext.TraceState().String(),
			TraceFlags:             uint32(lk.SpanContext.TraceFlags()),
			Attributes:             convertAttributes(lk.Attributes),
			DroppedAttributesCount: uint32(lk.DroppedAttributeCount),
		})
	}

	scope := s.InstrumentationScope()
	return &proxypb.TraceSpan{
		TraceId:                     traceID,
		SpanId:                      spanID,
		ParentSpanId:                parentSpanID,
		TraceFlags:                  uint32(sc.TraceFlags()),
		TraceState:                  sc.TraceState().String(),
		Name:                        s.Name(),
		Kind:                        convertSpanKind(s.SpanKind()),
		StartTime:                   timestamppb.New(s.StartTime()),
		EndTime:                     timestamppb.New(s.EndTime()),
		Attributes:                  convertAttributes(s.Attributes()),
		Events:                      events,
		Links:                       links,
		StatusCode:                  convertStatusCode(s.Status().Code),
		StatusMessage:               s.Status().Description,
		InstrumentationScopeName:    scope.Name,
		InstrumentationScopeVersion: scope.Version,
		ChildSpanCount:              int32(s.ChildSpanCount()),
		DroppedAttributesCount:      uint32(s.DroppedAttributes()),
		DroppedEventsCount:          uint32(s.DroppedEvents()),
		DroppedLinksCount:           uint32(s.DroppedLinks()),
	}
}

func convertSpanKind(k otrace.SpanKind) proxypb.SpanKind {
	switch k {
	case otrace.SpanKindInternal:
		return proxypb.SpanKind_SPAN_KIND_INTERNAL
	case otrace.SpanKindServer:
		return proxypb.SpanKind_SPAN_KIND_SERVER
	case otrace.SpanKindClient:
		return proxypb.SpanKind_SPAN_KIND_CLIENT
	case otrace.SpanKindProducer:
		return proxypb.SpanKind_SPAN_KIND_PRODUCER
	case otrace.SpanKindConsumer:
		return proxypb.SpanKind_SPAN_KIND_CONSUMER
	default:
		return proxypb.SpanKind_SPAN_KIND_UNSPECIFIED
	}
}

func convertStatusCode(c otelcodes.Code) proxypb.SpanStatusCode {
	// Go otel/codes uses Error=1, Ok=2 while proxypb follows OTLP (Ok=1, Error=2).
	switch c {
	case otelcodes.Ok:
		return proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
	case otelcodes.Error:
		return proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	default:
		return proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	}
}

func convertAttributes(kvs []attribute.KeyValue) map[string]*proxypb.AttributeValue {
	if len(kvs) == 0 {
		return nil
	}
	out := make(map[string]*proxypb.AttributeValue, len(kvs))
	for _, kv := range kvs {
		v := kv.Value
		var pv *proxypb.AttributeValue
		switch v.Type() {
		case attribute.STRING:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: v.AsString()}}
		case attribute.INT64:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v.AsInt64()}}
		case attribute.FLOAT64:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_DoubleValue{DoubleValue: v.AsFloat64()}}
		case attribute.BOOL:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v.AsBool()}}
		case attribute.STRINGSLICE:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringList{StringList: &proxypb.StringList{Values: v.AsStringSlice()}}}
		case attribute.INT64SLICE:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64List{Int64List: &proxypb.Int64List{Values: v.AsInt64Slice()}}}
		case attribute.FLOAT64SLICE:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_DoubleList{DoubleList: &proxypb.DoubleList{Values: v.AsFloat64Slice()}}}
		case attribute.BOOLSLICE:
			pv = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolList{BoolList: &proxypb.BoolList{Values: v.AsBoolSlice()}}}
		}
		if pv != nil {
			out[string(kv.Key)] = pv
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
