// Package wire is the single source of the JetStream stream layout, SDK headers,
// and connection setup shared by producer and consumer.
package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Headers the SDK owns. The Nats- prefix is reserved for headers the NATS server
// interprets, so the SDK never sets Nats-Msg-Id.
const (
	// HeaderKey carries the message key set by the publisher.
	HeaderKey = "Sdk-Msg-Key"
	// HeaderTag carries the message tag set by the publisher.
	HeaderTag = "Sdk-Msg-Tag"
)

// defaultPort is the NATS client port used when an endpoint omits one.
const defaultPort = 4222

// Options holds the settings for Connect. Only the auth fields that are set are
// applied.
type Options struct {
	// Endpoints lists the NATS server addresses, joined with commas when dialing.
	Endpoints []string
	// Timeout bounds each connection attempt.
	Timeout time.Duration
	// Token is the access token for token authentication.
	Token string
	// Username is the user for userinfo authentication, used with Password.
	Username string
	// Password is the password for userinfo authentication, used with Username.
	Password string
	// NKey is the path to an NKey user seed file.
	NKey string
	// CredentialsFile is the path to a NATS credentials file.
	CredentialsFile string
}

// DataSubjects returns the subjects that carry the data messages of topic. The
// consumer filters on these, so schedule messages are never delivered.
func DataSubjects(topic string) []string {
	return []string{topic, topic + ".>"}
}

// StreamConfig returns the stream layout for topic: the data subjects, the
// schedule subjects, and AllowMsgSchedules. Every other field keeps the server
// default.
func StreamConfig(topic string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              topic,
		Subjects:          append(DataSubjects(topic), scheduleWildcard(topic)),
		AllowMsgSchedules: true,
	}
}

// ScheduleSubject returns a unique subject for one delayed message of topic. Each
// schedule needs its own subject, because a second schedule on a shared subject
// replaces the first.
func ScheduleSubject(topic string) string {
	return "schedules." + topic + "." + nuid.Next()
}

// PatchStream returns a copy of cfg with the SDK-owned fields of the topic layout
// applied. An SDK subject already covered by an existing subject is skipped. An
// existing subject that an SDK wildcard covers is removed, because JetStream
// rejects overlapping subjects in one stream. Other existing subjects keep their
// order, and new subjects are appended. AllowMsgSchedules is set to true. Enabling
// schedules makes the server also turn AllowRollup on. Every other field is kept.
// The bool reports whether anything changed. The input slice is not modified.
//
// It returns an error when the stream cannot take message schedules: DenyPurge,
// Discard set to DiscardNew, Mirror, Sources, or AllowMsgCounter.
func PatchStream(cfg jetstream.StreamConfig, topic string) (jetstream.StreamConfig, bool, error) {
	if bad := scheduleIncompatibility(cfg); bad != "" {
		return cfg, false, fmt.Errorf("wire: stream %s: %s is incompatible with message schedules", topic, bad)
	}

	changed := false
	subjects := slices.Clone(cfg.Subjects)
	for _, s := range StreamConfig(topic).Subjects {
		if slices.ContainsFunc(subjects, func(e string) bool { return covers(e, s) }) {
			continue
		}
		subjects = slices.DeleteFunc(subjects, func(e string) bool { return covers(s, e) })
		subjects = append(subjects, s)
		changed = true
	}
	if !cfg.AllowMsgSchedules {
		cfg.AllowMsgSchedules = true
		changed = true
	}
	cfg.Subjects = subjects
	return cfg, changed, nil
}

// scheduleIncompatibility names the first setting of cfg that the server rejects
// together with message schedules, or returns "" when there is none.
func scheduleIncompatibility(cfg jetstream.StreamConfig) string {
	switch {
	case cfg.DenyPurge:
		return "DenyPurge"
	case cfg.Discard == jetstream.DiscardNew:
		return "Discard policy DiscardNew"
	case cfg.Mirror != nil:
		return "Mirror"
	case len(cfg.Sources) > 0:
		return "Sources"
	case cfg.AllowMsgCounter:
		return "AllowMsgCounter"
	}
	return ""
}

// covers reports whether the subject wide matches every subject that narrow
// matches. It follows NATS token rules: * matches one token, and > matches one or
// more trailing tokens. Literal tokens must be equal.
func covers(wide, narrow string) bool {
	w := strings.Split(wide, ".")
	n := strings.Split(narrow, ".")
	for i, tok := range w {
		if tok == ">" {
			return i < len(n)
		}
		if i >= len(n) || n[i] == ">" {
			return false
		}
		if tok != "*" && tok != n[i] {
			return false
		}
	}
	return len(w) == len(n)
}

// ServerAddrPort splits the first endpoint into its host and port. An endpoint may
// be host:port or nats://host:port. The port defaults to 4222. It returns an empty
// host and port 0 when endpoints is empty.
func ServerAddrPort(endpoints []string) (string, int) {
	if len(endpoints) == 0 {
		return "", 0
	}
	ep := endpoints[0]
	if strings.Contains(ep, "://") {
		u, err := url.Parse(ep)
		if err == nil {
			port, _ := strconv.Atoi(u.Port())
			if port == 0 {
				port = defaultPort
			}
			return u.Hostname(), port
		}
	}
	host, portStr, err := net.SplitHostPort(ep)
	if err != nil {
		return ep, defaultPort
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, defaultPort
	}
	return host, port
}

// Connect dials the NATS servers in o.Endpoints and returns the connection. The
// client is named name. The dial is bounded by ctx: each attempt uses o.Timeout
// (nats.DefaultTimeout when zero), capped so that all endpoints fit before the
// deadline of ctx. It retries the first connection in the background if the
// server is down, so a nil error does not mean the connection is up. Use
// [nats.Conn.IsConnected] to check it.
//
// The error wraps the failure, such as an unreadable NKey seed file, and names
// the endpoints without their credentials.
func Connect(ctx context.Context, name string, o Options) (*nats.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	opts := []nats.Option{
		nats.Name(name),
		nats.Timeout(dialTimeout(ctx, o)),
		nats.RetryOnFailedConnect(true),
	}

	if o.Token != "" {
		opts = append(opts, nats.Token(o.Token))
	}
	if o.Username != "" || o.Password != "" {
		opts = append(opts, nats.UserInfo(o.Username, o.Password))
	}
	if o.NKey != "" {
		opt, err := nats.NkeyOptionFromSeed(o.NKey)
		if err != nil {
			return nil, fmt.Errorf("wire: nkey option: %w", err)
		}
		opts = append(opts, opt)
	}
	if o.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(o.CredentialsFile))
	}

	nc, err := nats.Connect(strings.Join(o.Endpoints, ","), opts...)
	if err != nil {
		// The text, not the error, is kept: the wrapped error can still hold the
		// URL of a parse failure with its userinfo.
		return nil, fmt.Errorf("wire: connect to %s: %s", redactEndpoints(o.Endpoints), redactUserinfo(err.Error()))
	}
	return nc, nil
}

// Drain closes nc after pending messages are flushed. It waits until the
// connection is closed, bounded by ctx. When ctx ends first it closes nc at once
// and returns ctx.Err(). A connection that is already closed is not an error.
func Drain(ctx context.Context, nc *nats.Conn) error {
	if err := nc.Drain(); err != nil {
		if errors.Is(err, nats.ErrConnectionClosed) {
			return nil
		}
		nc.Close()
		return fmt.Errorf("wire: drain: %w", err)
	}

	// nats.Conn.Drain is asynchronous, so the wait polls IsClosed. A ClosedHandler
	// is not used because it would replace the handler of the caller.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !nc.IsClosed() {
		select {
		case <-ctx.Done():
			nc.Close()
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// dialTimeout returns the timeout of one dial in Connect. It starts from o.Timeout,
// or nats.DefaultTimeout when that is zero. nats.Connect dials every endpoint once,
// so with a deadline on ctx the timeout is capped at the time left per endpoint,
// with a floor of one millisecond.
func dialTimeout(ctx context.Context, o Options) time.Duration {
	d := o.Timeout
	if d == 0 {
		d = nats.DefaultTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		perEndpoint := time.Until(deadline) / time.Duration(max(len(o.Endpoints), 1))
		d = min(d, max(perEndpoint, time.Millisecond))
	}
	return d
}

// userinfoRE matches the userinfo of a URL in free text.
var userinfoRE = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s"'@]*@`)

// redactUserinfo removes the userinfo of every URL in msg.
func redactUserinfo(msg string) string {
	return userinfoRE.ReplaceAllString(msg, "$1")
}

// redactEndpoints joins endpoints with commas and removes the userinfo of each URL.
// An endpoint without a scheme is read as nats://. An endpoint that does not parse
// is replaced by a placeholder, so no credential reaches an error.
func redactEndpoints(endpoints []string) string {
	out := make([]string, len(endpoints))
	for i, ep := range endpoints {
		if !strings.Contains(ep, "://") {
			ep = "nats://" + ep
		}
		u, err := url.Parse(ep)
		if err != nil {
			out[i] = "<invalid endpoint>"
			continue
		}
		u.User = nil
		out[i] = u.String()
	}
	return strings.Join(out, ",")
}

// scheduleWildcard returns the subject that matches every schedule of topic.
func scheduleWildcard(topic string) string {
	return "schedules." + topic + ".>"
}
