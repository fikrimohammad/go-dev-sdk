package apiserver

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app/server"
	hertzconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Server wraps a Hertz server with standardized middleware.
type Server struct {
	hz            *server.Hertz
	metricsClient metrics.Client
	tracerClient  tracer.Client
}

// New creates a Hertz server with required observability clients.
// Base middlewares are registered in a fixed order:
//
//  1. RequestID   — generates or extracts X-Request-ID
//
//  2. PanicRecovery — catches panics, returns 500
//
//  3. Tracer      — creates OTel span (must be after PanicRecovery for trace on panic)
//
//  4. Logger      — logs request (must be after Tracer for trace_id)
//
//  5. Metrics     — records count + duration
//
//     srv, err := apiserver.New(cfg, mc, tc)
//     if err != nil {
//     // handle
//     }
//     return srv.Run()
func New(cfg Config, mc metrics.Client, tc tracer.Client) (*Server, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	opts := []hertzconfig.Option{server.WithHostPorts(cfg.Addr)}
	if cfg.ReadTimeout > 0 {
		opts = append(opts, server.WithReadTimeout(cfg.ReadTimeout))
	}
	if cfg.WriteTimeout > 0 {
		opts = append(opts, server.WithWriteTimeout(cfg.WriteTimeout))
	}

	s := &Server{
		hz:            server.New(opts...),
		metricsClient: mc,
		tracerClient:  tc,
	}
	s.registerMiddlewares()
	return s, nil
}

// Hertz returns the underlying Hertz server for route registration.
func (s *Server) Hertz() *server.Hertz {
	return s.hz
}

// Run starts the server. Blocks until shutdown.
func (s *Server) Run() error {
	return s.hz.Run()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.hz.Shutdown(ctx)
}

// registerMiddlewares sets up the middleware stack in the required order.
//
// Order matters:
//   - RequestID must be first so all downstream middlewares see the ID.
//   - PanicRecovery must be before Tracer so panics during span creation are caught.
//   - Tracer must be before Logger so Logger can read trace_id from the span context.
//   - Logger must be before Metrics so errors are logged before metrics are recorded.
func (s *Server) registerMiddlewares() {
	s.hz.Use(RequestID())
	s.hz.Use(PanicRecovery())
	s.hz.Use(Tracer(s.tracerClient))
	s.hz.Use(Logger())
	s.hz.Use(Metrics(s.metricsClient))
}
