// Package wire is the single source of the JetStream stream layout, SDK headers,
// and connection setup shared by producer and consumer.
package wire

import (
	"fmt"
	"net"
	"net/url"
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
// applied. Missing subjects are appended after the existing ones, which keeps the
// order and any operator-added subjects. AllowMsgSchedules is set to true. Every
// other field is kept. The bool reports whether anything changed. The input
// slice is not modified.
func PatchStream(cfg jetstream.StreamConfig, topic string) (jetstream.StreamConfig, bool) {
	changed := false
	subjects := slices.Clone(cfg.Subjects)
	for _, s := range StreamConfig(topic).Subjects {
		if !slices.Contains(subjects, s) {
			subjects = append(subjects, s)
			changed = true
		}
	}
	if !cfg.AllowMsgSchedules {
		cfg.AllowMsgSchedules = true
		changed = true
	}
	cfg.Subjects = subjects
	return cfg, changed
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
// client is named name. It retries the first connection in the background if the
// server is down, so a nil error does not mean the connection is up. Use
// [nats.Conn.IsConnected] to check it.
//
// The error wraps the failure, such as an unreadable NKey seed file, and names
// the endpoints.
func Connect(name string, o Options) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.Timeout(o.Timeout),
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

	endpoints := strings.Join(o.Endpoints, ",")
	nc, err := nats.Connect(endpoints, opts...)
	if err != nil {
		return nil, fmt.Errorf("wire: connect to %s: %w", endpoints, err)
	}
	return nc, nil
}

// scheduleWildcard returns the subject that matches every schedule of topic.
func scheduleWildcard(topic string) string {
	return "schedules." + topic + ".>"
}
