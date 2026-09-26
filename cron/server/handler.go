package server

import (
	"context"
	"errors"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	hertzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/fikrimohammad/go-dev-sdk/cron"
)

// RegisterRoutes registers the cron REST API routes on a Hertz engine.
func RegisterRoutes(h *hertzserver.Hertz, runner cron.JobRunner, prefix string) {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	h.GET("/healthz", healthzHandler)
	if prefix != "" {
		h.GET(prefix+"/healthz", healthzHandler)
	}

	jobsPath := prefix + "/jobs"
	jobItemPath := prefix + "/jobs/:name"
	jobRunPath := prefix + "/jobs/:name/run"

	h.GET(jobsPath, listJobsHandler(runner))
	h.GET(jobItemPath, getJobHandler(runner))
	h.POST(jobRunPath, triggerJobHandler(runner))
}

func healthzHandler(ctx context.Context, c *app.RequestContext) {
	c.JSON(consts.StatusOK, map[string]string{"status": "ok"})
}

func listJobsHandler(runner cron.JobRunner) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		jobs := runner.Jobs()
		c.JSON(consts.StatusOK, jobs)
	}
}

func getJobHandler(runner cron.JobRunner) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		job, ok := runner.Job(name)
		if !ok {
			c.JSON(consts.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		c.JSON(consts.StatusOK, job)
	}
}

func triggerJobHandler(runner cron.JobRunner) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		name := c.Param("name")
		if _, ok := runner.Job(name); !ok {
			c.JSON(consts.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}

		asyncVal := c.Query("async")
		isAsync := strings.EqualFold(asyncVal, "true") || asyncVal == "1"
		if isAsync {
			asyncCtx := context.WithoutCancel(ctx)
			go func() {
				_ = runner.Trigger(asyncCtx, name)
			}()
			c.JSON(consts.StatusAccepted, map[string]string{
				"name":   name,
				"status": "triggered",
				"mode":   "async",
			})
			return
		}

		err := runner.Trigger(ctx, name)
		if err == nil {
			c.JSON(consts.StatusOK, map[string]string{
				"name":   name,
				"status": "completed",
			})
			return
		}

		switch {
		case errors.Is(err, cron.ErrJobNotFound):
			c.JSON(consts.StatusNotFound, map[string]string{"error": "job not found"})
		case errors.Is(err, cron.ErrLockHeld):
			c.JSON(consts.StatusConflict, map[string]string{"error": "job is currently executing (lock held)"})
		case errors.Is(err, cron.ErrJobRunning):
			c.JSON(consts.StatusConflict, map[string]string{"error": "job is already running"})
		case errors.Is(err, cron.ErrJobDisabled):
			c.JSON(consts.StatusUnprocessableEntity, map[string]string{"error": "job is disabled by toggler"})
		case errors.Is(err, cron.ErrEngineStopped):
			c.JSON(consts.StatusServiceUnavailable, map[string]string{"error": "engine is stopped"})
		case errors.Is(err, context.DeadlineExceeded):
			c.JSON(consts.StatusGatewayTimeout, map[string]string{"error": "job execution timed out"})
		default:
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
}
