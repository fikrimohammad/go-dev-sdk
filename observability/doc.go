// Package observability unifies logging, metrics, and tracing behind the same
// global-facade design as the standard library's log/slog: a New constructor
// builds a configured client, SetDefault installs it as the package default,
// Default returns it, and the package-level funcs are thin wrappers over
// Default. Each signal is configured and shut down independently:
//
//	info := appinfo.Default()
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
//	metrics.SetDefault(mc)
//
//	tc, err := tracer.New(ctx, info, tracer.Config{Endpoint: "collector:4317"})
//	if err != nil {
//	    // handle
//	}
//	tracer.SetDefault(tc)
//	otel.SetTracerProvider(tc.Provider())
//
//	defer mc.Stop(ctx)
//	defer tc.Stop(ctx)
//
//	// Package-level funcs delegate to the installed default:
//	logs.Info(ctx, "hello", "user_id", 42)
//	metrics.Histogram(ctx, "http.server.request.duration", 0.05, attrs)
//	tc.Tracer("svc").Start(ctx, "operation")
//
// # Packages
//
//   - logs: routes structured records to stdout/stderr by severity and adds
//     trace_id/span_id from a valid span context.
//   - metrics: records counters, histograms, up-down counters, and gauges over
//     OTLP gRPC with instrument units, descriptions, and a stable scope.
//   - tracer: starts spans over OTLP gRPC. Registering the OTel global
//     provider and propagator is the composition root's job (see tracer.New).
//   - attributes: shared key normalization and resource-attribute helpers.
//
// # Construction vs. globals
//
// New is a global-free constructor: it validates config and returns a fresh,
// started client without mutating package defaults. SetDefault swaps that
// client into the package default (an atomic store, lock-free for readers),
// and Default returns the current default. The package-level funcs (logs.Info,
// metrics.Count, tracer.Tracer) delegate to Default, so code that does not
// receive an injected client can call the same API. tracer.TraceIDFrom is a
// pure context helper and does not require a default client.
//
// Each package's default is populated at init with a safe fallback — logs
// writes to a debug-level text logger on stderr, metrics and tracer are noop —
// so Default never returns nil and the package funcs are safe to call
// anywhere, before or after SetDefault. SetDefault(nil) restores that safe
// fallback.
//
// # Errors
//
// The package-level metrics funcs (Count, Histogram, Add, Record, AddUpDown,
// RecordGauge, Stop) return error and delegate to the installed client;
// callers should treat their result as a real error. The instrument-based
// funcs require the default to implement metrics.InstrumentClient and report
// metrics.ErrUnsupportedInstrument otherwise. logs.Logger methods and the logs
// package funcs return nothing, matching the Logger interface.
//
// # Lifecycle
//
// Ownership lives with the caller: whoever calls New is responsible for Stop.
// metrics and tracer clients are idempotent under Stop, and the composition
// root (e.g. app resource.Close) stops the concrete clients it built. The
// metrics.Client and tracer.Client interfaces are the injected-client
// contracts used by middleware; metrics.New returns a metrics.InstrumentClient
// and tracer.New returns a tracer.Client, both of which expose Stop and
// Provider.
//
// The appinfo.Info identity is attached to every signal as the OTel semantic
// convention resource attributes service.name, service.version, and
// deployment.environment. A non-empty service name is required; empty optional
// identity fields are omitted. Developer-style attribute keys are normalized to
// that style ("order_id" → "order.id"), while keys that already contain a dot
// and the standard correlation keys trace_id, span_id, and trace_flags keep
// their spelling. Metric instrument names are NOT namespaced by the service
// name; service identity lives solely in the resource attributes.
package observability
