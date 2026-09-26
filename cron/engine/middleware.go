package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	tracerScope       = "cron.job"
	metricJobRuns     = "cron.job.runs"
	metricJobDuration = "cron.job.duration"
)

// meta holds metadata and injected dependencies for a single job execution.
type meta struct {
	job     cron.Job
	trigger string // "scheduled" or "manual"
	timeout time.Duration

	toggler cron.Toggler
	locker  cron.Locker
	metrics metrics.Client
	tracer  tracer.Client
}

func (m meta) tracerOr() trace.Tracer {
	if m.tracer != nil {
		return m.tracer.Tracer(tracerScope)
	}
	return tracer.Tracer(tracerScope)
}

func (m meta) baseAttrs() map[string]any {
	return map[string]any{
		"job.name": m.job.Name,
		"trigger":  m.trigger,
	}
}

func (m meta) eventAttrs(ctx context.Context, err error) map[string]any {
	attrs := m.baseAttrs()
	if token, ok := cron.FencingTokenFromContext(ctx); ok {
		attrs["fencing_token"] = token
	}
	if err != nil {
		attrs["status"] = "failed"
		attrs["error.type"] = err.Error()
	} else {
		attrs["status"] = "success"
	}
	return attrs
}

func (m meta) panicRecovery(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				attrs := []any{
					"panic", r,
					"job", m.job.Name,
					"stack", string(stack),
				}
				if id := tracer.TraceIDFrom(ctx); id != "" {
					attrs = append(attrs, "trace_id", id)
				}
				logs.Error(ctx, "cron panic recovered", attrs...)
				err = fmt.Errorf("cron panic recovered: %v", r)
			}
		}()
		return next(ctx)
	}
}

func (m meta) tracerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		ctx, span := m.tracerOr().Start(ctx, "cron."+m.job.Name,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(tracer.Attrs(m.baseAttrs())...),
		)
		defer span.End()

		err := next(ctx)

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.SetAttributes(tracer.Attrs(m.eventAttrs(ctx, err))...)
		return err
	}
}

func (m meta) togglerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		if m.toggler != nil {
			enabled, err := m.toggler.IsEnabled(ctx, m.job.Name)
			if err != nil {
				logs.Warn(ctx, "failed to check job toggle; failing open", "job", m.job.Name, "error", err)
			} else if !enabled {
				logs.Info(ctx, "cron job skipped: disabled by toggler", "job", m.job.Name)
				return cron.ErrJobDisabled
			}
		}
		return next(ctx)
	}
}

func (m meta) lockerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		if m.locker != nil {
			lockKey := "cron:lock:" + m.job.Name
			lockTTL := m.timeout + FixedLockTTLBuffer
			token, err := m.locker.Lock(ctx, lockKey, lockTTL)
			if err != nil {
				if errors.Is(err, cron.ErrLockHeld) {
					logs.Debug(ctx, "cron job skipped: lock already held", "job", m.job.Name)
					return cron.ErrLockHeld
				}
				logs.Error(ctx, "cron job lock infrastructure error", "job", m.job.Name, "error", err)
				return fmt.Errorf("cron: lock failed for job %q: %w", m.job.Name, err)
			}
			defer func() {
				unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if uerr := token.Unlock(unlockCtx); uerr != nil {
					logs.Warn(ctx, "failed to release lock for job", "job", m.job.Name, "error", uerr)
				}
			}()

			ctx = cron.WithFencingToken(ctx, token.FencingToken())
		}
		return next(ctx)
	}
}

func (m meta) metricsMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)

		attrs := m.eventAttrs(ctx, err)
		duration := time.Since(start).Seconds()

		if m.metrics != nil {
			_ = m.metrics.Count(ctx, metricJobRuns, 1, attrs)
			_ = m.metrics.Histogram(ctx, metricJobDuration, duration, attrs)
		} else {
			_ = metrics.Count(ctx, metricJobRuns, 1, attrs)
			_ = metrics.Histogram(ctx, metricJobDuration, duration, attrs)
		}
		return err
	}
}

func (m meta) loggerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		start := time.Now()
		logs.Info(ctx, "cron job started", "job", m.job.Name, "trigger", m.trigger)

		err := next(ctx)

		duration := time.Since(start)
		attrs := []any{
			"job", m.job.Name,
			"trigger", m.trigger,
			"duration", duration,
		}
		if token, ok := cron.FencingTokenFromContext(ctx); ok {
			attrs = append(attrs, "fencing_token", token)
		}
		if id := tracer.TraceIDFrom(ctx); id != "" {
			attrs = append(attrs, "trace_id", id)
		}
		if err != nil {
			attrs = append(attrs, "error", err)
			logs.Error(ctx, "cron job failed", attrs...)
		} else {
			logs.Info(ctx, "cron job completed", attrs...)
		}
		return err
	}
}

// buildChain wraps the raw handler in the strict 6-stage middleware sequence:
// panic recovery -> tracer -> toggler -> distributed locker -> metrics -> logger -> handler.
func (m meta) buildChain(handler cron.HandlerFunc) cron.HandlerFunc {
	return m.panicRecovery(
		m.tracerMW(
			m.togglerMW(
				m.lockerMW(
					m.metricsMW(
						m.loggerMW(handler),
					),
				),
			),
		),
	)
}
