package rabbitmq

import (
    "context"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    traceapi "go.opentelemetry.io/otel/trace"
)

func TestTraceHeadersFromContext_InjectsTraceParent(t *testing.T) {
    tp := sdktrace.NewTracerProvider()
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})
    t.Cleanup(func() {
        _ = tp.Shutdown(context.Background())
    })

    tracer := otel.Tracer("test-tracer")
    ctx, span := tracer.Start(context.Background(), "test-span")
    defer span.End()

    headers := traceHeadersFromContext(ctx)
    require.Contains(t, headers, "traceparent")
    assert.NotEmpty(t, headers["traceparent"])
}

func TestTraceContextFromHeaders_ExtractsSameTraceID(t *testing.T) {
    tp := sdktrace.NewTracerProvider()
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.TraceContext{})
    t.Cleanup(func() {
        _ = tp.Shutdown(context.Background())
    })

    tracer := otel.Tracer("test-tracer")
    ctx, span := tracer.Start(context.Background(), "test-span")
    defer span.End()

    headers := traceHeadersFromContext(ctx)
    extractedCtx := traceContextFromHeaders(headers)

    extractedSpan := traceapi.SpanContextFromContext(extractedCtx)
    assert.True(t, extractedSpan.IsValid())
    assert.Equal(t, span.SpanContext().TraceID(), extractedSpan.TraceID())
    assert.Equal(t, span.SpanContext().SpanID(), extractedSpan.SpanID())
}
