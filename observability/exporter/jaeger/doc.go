// Package jaeger provides an OpenTelemetry SpanExporter that ships spans to
// a Jaeger collector over HTTP. It is a thin adapter around the upstream
// go.opentelemetry.io/otel/exporters/jaeger package, exposing a constructor
// (New) that resolves a collector endpoint and an Exporter wrapper that
// implements the OTel sdktrace.SpanExporter contract.
package jaeger
