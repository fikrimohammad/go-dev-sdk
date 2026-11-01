// Package client builds the HTTP client that SDK services use to call external
// vendors. It enforces the retry, deadline, and telemetry rules of the rest
// module, so every outbound call behaves the same. The middleware chain runs
// in this order, outermost first:
//
//	logging → retry → tracing → metrics → deadline → [team middleware] → Hertz transport
//
// New returns the plain Hertz client, with no wrapper type. Teams add
// per-attempt middleware with the client's Use method. That middleware runs
// innermost, once for each attempt, which is what request signing needs,
// because a signature with a timestamp must be made again on every retry.
//
// Teams must not call SetRetryIfFunc, must not set WithRetryConfig, and must
// not add retry on top of a RoundTripper. Retry already runs in the chain, so
// another layer would multiply the attempts and send non-idempotent requests
// again. This is documented, not enforced.
//
// A Hertz client that New did not build has no policy: no retry, no
// per-attempt deadline, and no telemetry.
//
// Each attempt gets one client span and one http.client.request.duration
// measurement. No trace context or request ID is sent to a vendor, and telemetry
// carries no URL query values or userinfo.
//
// Proxy credentials go only to the proxy. For http targets, Hertz sets
// Proxy-Authorization on the caller's request and leaves it there, so do not
// log or reuse request headers.
//
// Hertz's global hlog logs full URLs, query strings included, at Warn when it
// re-dials a closed pooled connection. This package does not change global
// state, so applications should route hlog through a redacting logger or set
// its level above Warn.
//
// Paths are sent exactly as given. Hertz's path normalization is off, so an
// escaped slash, a double slash, or a dot segment reaches the vendor unchanged.
//
// A vendor that answers with a 1xx status other than 100 Continue fails the
// attempt with ErrInformationalResponse. Hertz returns that response as final
// and leaves the real one on the connection, so the client also closes its
// idle connections. A concurrent call can still take that connection in the
// window before the close, which is a Hertz gap.
package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Option configures a client returned by [New].
type Option func(*options)

// options holds the telemetry clients set by Option. A nil client means the
// package-level default.
type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics sets the metrics client for the client's measurements. Without
// it, or with a nil m, the client uses the OTel global meter provider
// (otel.SetMeterProvider), read on every attempt.
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer sets the tracer client for the client's spans. Without it, or
// with a nil t, the client uses the OTel global tracer provider
// (otel.SetTracerProvider or tracer.SetGlobal), read on every attempt.
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// New builds a Hertz client from cfg, which is defaulted and validated first,
// and applies opts. It returns an error if cfg is invalid.
//
// A call that times out, whether from its context, Config.Timeout, or
// Config.AttemptTimeout, returns an error that matches
// errors.Is(err, context.DeadlineExceeded). Retry covers the idempotent
// methods and requests with an Idempotency-Key, on transport errors that can
// succeed later and on 429, 502, 503, and 504, up to MaxAttempts. A final 503
// is returned with a nil error, as in net/http.
//
// Every request through the client goes through cfg.Proxy when it is set.
// Hertz's own retry is left off, so retry happens only in the middleware
// chain. Telemetry names the vendor, not the proxy, as the server address.
// Paths are sent as given, because New turns off Hertz's path normalization.
func New(cfg Config, opts ...Option) (*hclient.Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// WithTLSConfig is always applied. It also selects the standard dialer,
	// which is the only one that does TLS here: without it Hertz uses netpoll,
	// and every https request fails with "not support tls". The dialer comes
	// after it, so the deadline wrapper replaces the standard dialer.
	c, err := hclient.NewClient(
		hclient.WithDialTimeout(cfg.DialTimeout),
		hclient.WithTLSConfig(cfg.TLS),
		hclient.WithDialer(deadlineDialer{standard.NewDialer()}),
		// Hertz decodes and cleans paths by default, so an escaped slash or a dot
		// segment would reach the vendor as a different path than the caller sent.
		hclient.WithDisablePathNormalizing(true),
	)
	if err != nil {
		return nil, fmt.Errorf("rest/client: new hertz client: %w", err)
	}
	if cfg.Proxy != "" {
		uri, err := proxyURI(cfg.Proxy)
		if err != nil {
			return nil, err
		}
		c.SetProxy(protocol.ProxyURI(uri))
	}
	registerMiddleware(c, cfg, o.metrics, o.tracer)
	return c, nil
}

// deadlineDialer gives every new connection a deadline right after it is
// dialed. Hertz runs the proxy CONNECT under its own 60s context and sets no
// connection deadline, so a proxy that accepts TCP and never answers would
// hold the call past the caller's deadline. The dial timeout passed by Hertz
// is already capped by the request timeout, so the deadline follows the
// caller's deadline.
type deadlineDialer struct{ network.Dialer }

// DialConnection dials like the embedded dialer, then sets a deadline of
// timeout on the new connection, unless the dial failed or timeout is not
// positive.
func (d deadlineDialer) DialConnection(n, addr string, timeout time.Duration, tlsCfg *tls.Config) (network.Conn, error) { //nolint:ireturn // network.Dialer fixes this return type
	conn, err := d.Dialer.DialConnection(n, addr, timeout, tlsCfg)
	if err == nil && timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}
	return conn, err
}

// proxyURI builds the Hertz proxy address from a proxy URL that validate has
// already accepted. Hertz reads the credentials from the URI setters and does
// not decode them, so they are set from the decoded userinfo of url.URL.
func proxyURI(proxy string) (*protocol.URI, error) {
	u, err := url.Parse(proxy)
	if err != nil {
		return nil, errors.New("rest/client: proxy must be a valid URL")
	}
	uri := protocol.ParseURI(u.Scheme + "://" + u.Host)
	if u.User != nil {
		uri.SetUsername(u.User.Username())
		password, _ := u.User.Password()
		uri.SetPassword(password)
	}
	return uri, nil
}

// registerMiddleware installs the SDK middleware on c, outermost first.
// Tracing and metrics sit outside deadline, so they see the wrapped timeout
// error of each attempt. Team middleware added later with Use runs inside
// deadline, once per attempt.
//
//	c.Use(logging(cfg), retry(cfg), tracing(tc), measure(mc), deadline(cfg, c))
func registerMiddleware(c *hclient.Client, cfg Config, mc metrics.Client, tc tracer.Client) {
	c.Use(logging(cfg), retry(cfg), tracing(tc), measure(mc), deadline(cfg, c))
}
