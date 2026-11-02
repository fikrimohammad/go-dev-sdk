package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/attribute"
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

var (
	metricJobRunsInstrument = metrics.Instrument{
		Name:        metricJobRuns,
		Unit:        "{run}",
		Description: "Cron job runs",
	}
	metricJobDurationInstrument = metrics.Instrument{
		Name:        metricJobDuration,
		Unit:        "s",
		Description: "Cron job duration",
	}
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
				// Fail open: run the job and note the toggler error on the span.
				trace.SpanFromContext(ctx).AddEvent("cron.toggler.error",
					trace.WithAttributes(attribute.String("error.message", err.Error())))
			} else if !enabled {
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
			token, lockErr := m.locker.Lock(ctx, lockKey, lockTTL)
			if lockErr != nil {
				if errors.Is(lockErr, cron.ErrLockHeld) {
					return cron.ErrLockHeld
				}
				return fmt.Errorf("cron: lock job %q: %w", m.job.Name, lockErr)
			}
			// Release even when ctx is canceled, and keep its trace values. A
			// failed unlock doesn't change the job's outcome: the lock expires
			// with its TTL, so the failure is only noted on the span.
			defer func() {
				unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if uerr := token.Unlock(unlockCtx); uerr != nil {
					trace.SpanFromContext(ctx).AddEvent("cron.unlock.error",
						trace.WithAttributes(attribute.String("error.message", uerr.Error())))
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
			_ = m.metrics.Add(ctx, metricJobRunsInstrument, 1, attrs)
			_ = m.metrics.Record(ctx, metricJobDurationInstrument, duration, attrs)
		} else {
			_ = metrics.Add(ctx, metricJobRunsInstrument, 1, attrs)
			_ = metrics.Record(ctx, metricJobDurationInstrument, duration, attrs)
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
		switch {
		case err == nil:
			logs.Info(ctx, "cron job completed", attrs...)
		case errors.Is(err, cron.ErrJobDisabled), errors.Is(err, cron.ErrLockHeld), errors.Is(err, cron.ErrJobRunning):
			logs.Info(ctx, "cron job skipped", append(attrs, "reason", err.Error())...)
		default:
			logs.Error(ctx, "cron job failed", append(attrs, "error", err)...)
		}
		return err
	}
}

// buildChain wraps the raw handler in the middleware sequence:
// tracer -> logger -> toggler -> distributed locker -> metrics -> panic recovery -> handler.
// Panic recovery is innermost, so a panic is traced, logged, and measured as a
// failed run. Only the logger logs the outcome: the toggler and locker return
// their errors, and a skip is logged at Info and a failure at Error.
func (m meta) buildChain(handler cron.HandlerFunc) cron.HandlerFunc {
	return m.tracerMW(
		m.loggerMW(
			m.togglerMW(
				m.lockerMW(
					m.metricsMW(
						m.panicRecovery(handler),
					),
				),
			),
		),
	)
}
