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
//   - logs: routes structured records to stdout/stderr by severity.
//   - metrics: records counters and histograms over OTLP gRPC.
//   - tracer: starts spans over OTLP gRPC. Registering the OTel global
//     provider and propagator is the composition root's job (see tracer.New).
//   - attributes: shared key normalization and resource-attribute helpers.
//
// # Construction vs. globals
//
// New is a pure constructor: it validates config and returns a fresh,
// started client with no global side effects. SetDefault swaps that client
// into the package default (an atomic store, lock-free for readers), and
// Default returns the current default. The package-level funcs (logs.Info,
// metrics.Count, tracer.Tracer, tracer.TraceIDFrom) delegate to Default, so
// code that does not receive an injected client can call the same API.
//
// Each package's default is populated at init with a safe fallback — logs
// writes to a debug-level text logger on stderr, metrics and tracer are noop —
// so Default never returns nil and the package funcs are safe to call
// anywhere, before or after SetDefault.
//
// # Errors
//
// The package-level metrics funcs (Count, Histogram, Stop) return error and
// delegate to the installed client, mirroring the metrics.Client interface;
// callers should treat their result as a real error. logs.Logger methods and
// the logs package funcs return nothing, matching the Logger interface.
//
// # Lifecycle
//
// Ownership lives with the caller: whoever calls New is responsible for Stop.
// metrics and tracer clients are idempotent under Stop, and the composition
// root (e.g. app resource.Close) stops the concrete clients it built. The
// metrics.Client and tracer.Client interfaces are the injected-client
// contracts used by middleware; tracer.New returns a tracer.Client that also
// exposes Stop and Provider.
//
// The appinfo.Info identity is attached to every signal as the OTel semantic
// convention resource attributes service.name, service.version, and
// deployment.environment, and attribute keys are normalized to that style
// ("order_id" → "order.id"). Metric instrument names are NOT namespaced by the
// service name; service identity lives solely in the resource attributes.
package observability
