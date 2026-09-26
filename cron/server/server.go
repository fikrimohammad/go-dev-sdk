package server

import (
	"context"
	"errors"

	hertzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/fikrimohammad/go-dev-sdk/apiserver"
	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Server wraps an apiserver.Server to provide developer management endpoints for cron jobs.
type Server struct {
	apiSrv *apiserver.Server
	runner cron.JobRunner
}

// New creates and initializes a Server.
func New(cfg Config, runner cron.JobRunner, mc metrics.Client, tc tracer.Client) (*Server, error) {
	if runner == nil {
		return nil, errors.New("cron/server: runner must not be nil")
	}

	apiSrv, err := apiserver.New(cfg.Config, mc, tc)
	if err != nil {
		return nil, err
	}

	RegisterRoutes(apiSrv.Hertz(), runner, cfg.Prefix)

	return &Server{
		apiSrv: apiSrv,
		runner: runner,
	}, nil
}

// Hertz returns the underlying Hertz server engine.
func (s *Server) Hertz() *hertzserver.Hertz {
	return s.apiSrv.Hertz()
}

// Run starts the server. Blocks until shutdown.
func (s *Server) Run() error {
	return s.apiSrv.Run()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.apiSrv.Shutdown(ctx)
}
