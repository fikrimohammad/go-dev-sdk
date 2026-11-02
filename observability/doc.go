// Package observability unifies logging, metrics, and tracing for Go services.
// Each signal is configured and shut down independently. Logs keep a package
// default: logs.SetDefault installs a client, logs.Default returns it, and the
// logs package funcs delegate to it. Metrics and traces follow the OpenTelemetry
// globals instead. New builds a client, the caller installs it into the globals,
// and every SDK package records through them:
//
//	info := identity.Default()
//	log, err := logs.New(info, logs.Config{Format: "json"})
//	if err != nil {
//	    // handle
//	}
//	logs.SetDefault(log)
//
//	mc, err := metrics.New(ctx, info, metrics.Config{Endpoint: "collector:4317"})
//	if err != nil {
//	    // handle
//	}
//	otel.SetMeterProvider(mc.Provider())
//
//	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317"})
//	if err != nil {
//	    // handle
//	}
//	tracer.SetGlobal(tc)
//
//	defer mc.Stop(ctx)
//	defer tc.Stop(ctx)
//
//	logs.Info(ctx, "hello", "user_id", 42)
//	metrics.Record(ctx, metrics.Instrument{Name: "http.server.request.duration", Unit: "s"}, 0.05, attrs)
//	tracer.Tracer("svc").Start(ctx, "operation")
//
// # Packages
//
//   - logs: routes structured records to stdout/stderr by severity and adds
//     trace_id/span_id from a valid span context.
//   - metrics: records counters, histograms, up-down counters, and gauges over
//     OTLP gRPC with instrument units, descriptions, and a stable scope.
//   - tracer: starts spans over OTLP gRPC with configurable sampler, batch,
//     and resource options. SetGlobal installs the provider and the W3C
//     propagator.
//   - attributes: shared key normalization and resource-attribute helpers.
//
// # Routing through the OTel globals
//
// New is a global-free constructor: it validates the config and returns a
// started client without touching any global state. Installing the client is a
// separate, explicit step. For metrics, pass the client's provider to
// otel.SetMeterProvider. For traces, call tracer.SetGlobal, which installs the
// provider and the W3C TraceContext and Baggage propagator.
//
// The SDK packages (db, redis, nats, cron, s3, and rest) and the package-level
// funcs (metrics.Add, metrics.Record, metrics.AddUpDown, metrics.RecordGauge,
// and tracer.Tracer) record through metrics.Default and tracer.Default with no
// other wiring. Default resolves the current OTel global on every call, so a
// provider installed later, or swapped for another later, takes effect on the
// next call. Before any install the OTel global is a no-op, and measurements and
// spans are dropped without error. tracer.TraceIDFrom and tracer.SpanIDFrom are
// pure context helpers and need no installed client.
//
// Logs are different: logs.Default is never nil. Before SetDefault is called,
// it is a debug-level text logger on stderr.
//
// # Errors
//
// The package-level metrics funcs (Add, Record, AddUpDown, RecordGauge) return
// error and delegate to the metrics.Client that Default resolves; callers should
// treat their result as a real error. logs.Logger methods and the logs package
// funcs return nothing, matching the Logger interface.
//
// # Lifecycle
//
// The caller owns each client that New returns. Call Stop on it at shutdown,
// for example with defer mc.Stop(ctx) and defer tc.Stop(ctx). Stop flushes
// pending telemetry and is idempotent. The package-level funcs have no Stop, and
// the SDK never stops a client it did not build. The metrics.Client and
// tracer.Client interfaces are the injected-client contracts used by middleware;
// both expose Stop and Provider.
//
// # Transport security
//
// Exporters use TLS by default. Config.Insecure is false in the zero value, so a
// plaintext collector requires Insecure: true. The OTEL_EXPORTER_OTLP_INSECURE
// environment variable can also enable plaintext.
//
// The service identity held in identity.Info is attached to every signal as the
// OTel semantic convention resource attributes service.name, service.version,
// and deployment.environment.name. A non-empty service name is required; empty
// optional identity fields are omitted. Developer-style attribute keys are
// normalized to that style ("order_id" → "order.id"), while keys that already
// contain a dot and the standard correlation keys trace_id, span_id, and
// trace_flags keep their spelling. Metric instrument names are NOT namespaced
// by the service name; service identity lives solely in the resource
// attributes.
package observability
