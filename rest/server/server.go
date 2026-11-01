// Package server builds the HTTP server for SDK services. Every request that
// Hertz routes, and every request that matches no route or the wrong method,
// goes through a fixed middleware stack that enforces the telemetry contract
// and the problem+json error format. The stack runs in this order:
//
//	requestID → tracing → metrics → logging → recovery → team middleware → handler
//
// Requests that Hertz rejects before routing never reach the stack. These are
// a malformed request line and oversized headers, which get Hertz's own
// response with no request ID, span, metric, or access-log line.
//
// Team middleware is added with [Server.Hertz] and its Use method. It runs
// inside recovery, so a panic in it is recovered and answered with a 500
// problem. Hertz only appends middleware, so the standard layers can't be
// removed or reordered. Hertz applies Use only to routes registered after the
// call, so add team middleware before registering routes, or attach it to a
// route group.
//
// Trailing slashes are not redirected. A request for "/users/42/" against the
// route "/users/:id" gets a 404 problem from the stack, so it is recorded like
// any other unmatched request.
package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Server serves HTTP requests through the standard middleware stack. Create
// it with [New].
type Server struct {
	hz  *server.Hertz
	cfg Config
	mc  metrics.Client
	tc  tracer.Client
}

// Option configures a Server returned by [New].
type Option func(*Server)

// WithMetrics sets the metrics client for the server's measurements. Without
// it, or with a nil m, the server uses the package-level default
// (metrics.SetDefault), read on every request.
func WithMetrics(m metrics.Client) Option {
	return func(s *Server) { s.mc = m }
}

// WithTracer sets the tracer client for the server's spans. Without it, or
// with a nil t, the server uses the package-level default (tracer.SetDefault),
// read on every request.
func WithTracer(t tracer.Client) Option {
	return func(s *Server) { s.tc = t }
}

// New builds a Server from cfg, which is defaulted and validated first, and
// applies opts. It returns an error if cfg is invalid.
func New(cfg Config, opts ...Option) (*Server, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Server{
		hz: server.New(
			server.WithHostPorts(cfg.Addr),
			server.WithReadTimeout(cfg.ReadTimeout),
			server.WithWriteTimeout(cfg.WriteTimeout),
			server.WithExitWaitTime(cfg.ShutdownTimeout),
			server.WithDisablePrintRoute(true),
			server.WithHandleMethodNotAllowed(true),
			// A redirect would be sent by Hertz before the stack runs, so it
			// is turned off and the 404 comes from the stack.
			server.WithRedirectTrailingSlash(false),
		),
		cfg: cfg,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerMiddleware()
	return s, nil
}

// registerMiddleware installs the standard stack, in the order the package doc
// gives, and the problem+json handlers for unmatched routes and methods. Hertz
// runs global middleware around the NoRoute and NoMethod handlers too, so the
// stack sees 404 and 405 responses.
func (s *Server) registerMiddleware() {
	s.hz.Use(
		requestID(),
		tracing(s.tc),
		measure(s.mc),
		logging(),
		recovery(),
	)
	s.hz.NoRoute(noRoute())
	s.hz.NoMethod(noMethod())
}

// requestIDKey is the context key under which requestID stores the request
// ID.
type requestIDKey struct{}

// RequestIDFrom returns the request ID of the request that ctx belongs to, or
// "" when ctx carries none. The ID is the incoming X-Request-ID when it is
// valid, and a generated one otherwise. The same value is set on the
// X-Request-ID response header and in the request_id field of problem bodies.
// A handler can overwrite the header, but not the request_id of a problem
// body.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// Hertz returns the underlying Hertz server. Use it to register routes, and to
// add team middleware with its Use method, which runs inside the standard
// stack. Hertz applies Use only to routes registered after the call, so call
// Use before registering routes, or attach the middleware to a route group.
func (s *Server) Hertz() *server.Hertz {
	return s.hz
}

// shutdownPoll is how often Run checks for the listener, and how often it
// calls Close while it waits for the serving goroutine to end.
const shutdownPoll = 10 * time.Millisecond

// Run serves requests until ctx is cancelled, then shuts the server down
// gracefully. It returns nil after a graceful shutdown. The shutdown waits at
// most [Config.ShutdownTimeout] for in-flight requests. If they outlast it,
// Run returns an error that wraps [context.DeadlineExceeded]. Those requests
// are not waited for, and their handlers may still be running when Run
// returns. Run returns an error if the listener can't be created, for example
// because another listener holds the address. Run returns only after the
// goroutine that serves requests has ended.
func (s *Server) Run(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- serve(s.hz) }()

	select {
	case err := <-errc:
		return runError(err)
	case <-ctx.Done():
	}

	tick := time.NewTicker(shutdownPoll)
	defer tick.Stop()
	// Hertz refuses Shutdown until the engine is running. A refused Shutdown
	// would leave the server serving with nothing waiting on it, so wait for
	// the listener first. A bind failure ends the wait.
	for !s.hz.IsRunning() {
		select {
		case err := <-errc:
			return runError(err)
		case <-tick.C:
		}
	}

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.hz.Shutdown(sctx)
	if err == nil && sctx.Err() != nil {
		// Hertz returns nil when the shutdown deadline expires, so the context
		// is the only signal that in-flight requests were not drained.
		err = sctx.Err()
	}

	// IsRunning can be true before netpoll has created its event loop server,
	// and Shutdown does nothing then. It returns nil and the server starts
	// serving with nothing stopping it. Keep calling Close until the serving
	// goroutine ends. Close is safe after a real shutdown: Engine.Close calls
	// the transport's Close, which is Shutdown with an expired context, and
	// netpoll's Shutdown returns nil once it has cleared its server. In the
	// window case Close doesn't wait for requests that started after the
	// refused Shutdown, so those requests are not waited for.
	var runErr error
	stopped := false
	for !stopped {
		select {
		case runErr = <-errc:
			stopped = true
		case <-tick.C:
			_ = s.hz.Close()
		}
	}
	if err != nil {
		return fmt.Errorf("rest/server: shutdown: %w", err)
	}
	return runError(runErr)
}

// serve runs hz.Run on the calling goroutine and turns a panic into an error.
// netpoll, Hertz's default transport, panics instead of returning when it
// can't create the listener, so the panic is contained here and reported
// through Run.
func serve(hz *server.Hertz) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("hertz panicked: %v", r)
		}
	}()
	return hz.Run()
}

// runError wraps an error from the serving goroutine with the rest/server
// prefix. It returns nil for nil.
func runError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rest/server: run: %w", err)
}
