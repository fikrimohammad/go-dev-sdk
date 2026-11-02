package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

const (
	// tracerScope is the OTel instrumentation scope name.
	tracerScope = "cron.job"

	// metricJobDuration is the OTel metric name, one histogram sample per run.
	metricJobDuration = "cron.job.duration"

	// attrJobName is the job name attribute on the span and the metric.
	attrJobName = "cron.job.name"

	// attrJobTrigger is the trigger attribute: "scheduled" or "manual".
	attrJobTrigger = "cron.job.trigger"

	// attrJobFencingToken is the fencing token attribute, on the span only.
	attrJobFencingToken = "cron.job.fencing_token"

	// attrJobSkipReason is the skip reason on the cron.job.skipped event:
	// "disabled", "lock_held", or "running".
	attrJobSkipReason = "cron.job.skip_reason"

	// eventJobSkipped is the span event that records a skipped run.
	eventJobSkipped = "cron.job.skipped"

	// errorTypeTimeout is the error.type for a deadline or a timeout.
	errorTypeTimeout = "timeout"

	// errorTypeCanceled is the error.type for a canceled run.
	errorTypeCanceled = "canceled"

	// errorTypePanic is the error.type for a recovered panic.
	errorTypePanic = "panic"

	// errorTypeOther is the error.type for any other failure.
	errorTypeOther = "_OTHER"
)

// errPanic marks an error made from a recovered panic.
var errPanic = errors.New("cron panic recovered")

// metricJobDurationInstrument is the cron.job.duration histogram.
var metricJobDurationInstrument = metrics.Instrument{
	Name:        metricJobDuration,
	Unit:        "s",
	Description: "Duration of cron job runs.",
}

// meta holds metadata and injected dependencies for a single job execution.
type meta struct {
	job     cron.Job
	trigger string // "scheduled" or "manual"
	timeout time.Duration

	// inFlight is the engine's set of job names that are running. The overlap
	// guard uses it, and a nil set disables the guard.
	inFlight *sync.Map

	toggler cron.Toggler
	locker  cron.Locker
	metrics metrics.Client
	tracer  tracer.Client
}

// runInfo holds per-run values that an inner middleware sets and the logger
// reads after the run. The logger puts it in the context.
type runInfo struct {
	fencingToken string
}

// runInfoKey is the context key for the run's runInfo.
type runInfoKey struct{}

// runInfoFrom returns the run's runInfo, or nil when the logger did not set one.
func runInfoFrom(ctx context.Context) *runInfo {
	info, _ := ctx.Value(runInfoKey{}).(*runInfo)
	return info
}

// metricsClient returns the injected metrics client, or metrics.Default() when
// none was injected. It is resolved per call so a later otel.SetMeterProvider
// still applies.
func (m meta) metricsClient() metrics.Client {
	if m.metrics != nil {
		return m.metrics
	}
	return metrics.Default()
}

// tracerClient returns the injected tracer client, or tracer.Default() when
// none was injected. It is resolved per call so a later otel.SetTracerProvider
// still applies.
func (m meta) tracerClient() tracer.Client {
	if m.tracer != nil {
		return m.tracer
	}
	return tracer.Default()
}

// baseAttrs returns the attributes shared by the span and the metric: the job
// name and trigger, plus error.type when err is non-nil.
func (m meta) baseAttrs(err error) map[string]any {
	attrs := map[string]any{
		attrJobName:    m.job.Name,
		attrJobTrigger: m.trigger,
	}
	if err != nil {
		attrs[string(otelsemconv.ErrorTypeKey)] = errorType(err)
	}
	return attrs
}

// skipReason returns the skip reason for a skip sentinel: "disabled",
// "lock_held", or "running". It reports false for any other error, including nil.
func skipReason(err error) (string, bool) {
	switch {
	case errors.Is(err, cron.ErrJobDisabled):
		return "disabled", true
	case errors.Is(err, cron.ErrLockHeld):
		return "lock_held", true
	case errors.Is(err, cron.ErrJobRunning):
		return "running", true
	default:
		return "", false
	}
}

// isSkip reports whether err is a skip sentinel. A skipped run is not a failure
// and is not measured.
func isSkip(err error) bool {
	_, ok := skipReason(err)
	return ok
}

// errorType classifies err for error.type. It never returns err.Error(),
// whose text is unbounded. An error with a code (such as *errs.Error) gives
// its code, and the other values are fixed names.
func errorType(err error) string {
	if errors.Is(err, errPanic) {
		return errorTypePanic
	}
	if coded, ok := errors.AsType[interface {
		error
		Code() int
	}](err); ok {
		return strconv.Itoa(coded.Code())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorTypeTimeout
	}
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		return errorTypeTimeout
	}
	if errors.Is(err, context.Canceled) {
		return errorTypeCanceled
	}
	return errorTypeOther
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
				err = fmt.Errorf("%w: %v", errPanic, r)
			}
		}()
		return next(ctx)
	}
}

func (m meta) tracerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		ctx, span := m.tracerClient().Tracer(tracerScope).Start(ctx, "cron."+m.job.Name,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(tracer.Attrs(m.baseAttrs(nil))...),
		)
		defer span.End()

		err := next(ctx)

		// A skip is not a failure: the status stays unset, and the event says why.
		if reason, ok := skipReason(err); ok {
			span.AddEvent(eventJobSkipped, trace.WithAttributes(attribute.String(attrJobSkipReason, reason)))
			return err
		}
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(tracer.Attrs(m.baseAttrs(err))...)
		}
		return err
	}
}

// overlapMW skips the run when the same job is already running on this engine.
// The skip is returned as cron.ErrJobRunning, so the logger and tracer record it.
func (m meta) overlapMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		if m.inFlight != nil {
			if _, loaded := m.inFlight.LoadOrStore(m.job.Name, struct{}{}); loaded {
				return cron.ErrJobRunning
			}
			defer m.inFlight.Delete(m.job.Name)
		}
		return next(ctx)
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
			if token == nil {
				return fmt.Errorf("cron: lock job %q: locker returned no token", m.job.Name)
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

			fence := token.FencingToken()
			// The span is started outside this middleware, so the token is set on it
			// here rather than read back when the span ends. The logger reads it
			// from the run info after the run.
			trace.SpanFromContext(ctx).SetAttributes(attribute.String(attrJobFencingToken, fence))
			if info := runInfoFrom(ctx); info != nil {
				info.fencingToken = fence
			}
			ctx = cron.WithFencingToken(ctx, fence)
		}
		return next(ctx)
	}
}

func (m meta) metricsMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)

		// A skipped run is logged and traced, not measured.
		if isSkip(err) {
			return err
		}
		duration := time.Since(start).Seconds()
		_ = m.metricsClient().Record(ctx, metricJobDurationInstrument, duration, m.baseAttrs(err))
		return err
	}
}

func (m meta) loggerMW(next cron.HandlerFunc) cron.HandlerFunc {
	return func(ctx context.Context) error {
		start := time.Now()
		logs.Info(ctx, "cron job started", "job", m.job.Name, "trigger", m.trigger)

		info := &runInfo{}
		ctx = context.WithValue(ctx, runInfoKey{}, info)
		err := next(ctx)

		duration := time.Since(start)
		attrs := []any{
			"job", m.job.Name,
			"trigger", m.trigger,
			"duration", duration,
		}
		if info.fencingToken != "" {
			attrs = append(attrs, "fencing_token", info.fencingToken)
		}
		if id := tracer.TraceIDFrom(ctx); id != "" {
			attrs = append(attrs, "trace_id", id)
		}
		switch {
		case err == nil:
			logs.Info(ctx, "cron job completed", attrs...)
		case isSkip(err):
			logs.Info(ctx, "cron job skipped", append(attrs, "reason", err.Error())...)
		default:
			logs.Error(ctx, "cron job failed", append(attrs, "error", err)...)
		}
		return err
	}
}

// buildChain wraps the raw handler in the middleware sequence:
// tracer -> logger -> overlap guard -> toggler -> metrics -> distributed locker -> panic recovery -> handler.
// Panic recovery is innermost, so a panic is traced, logged, and measured as a
// failed run. Metrics sit outside the locker, so a lock-backend failure is
// measured, while the skip sentinels are not. Only the logger logs the outcome:
// a skip is logged at Info and a failure at Error.
func (m meta) buildChain(handler cron.HandlerFunc) cron.HandlerFunc {
	return m.tracerMW(
		m.loggerMW(
			m.overlapMW(
				m.togglerMW(
					m.metricsMW(
						m.lockerMW(
							m.panicRecovery(handler),
						),
					),
				),
			),
		),
	)
}
