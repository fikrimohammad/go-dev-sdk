package wire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamConfig(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  []string
	}{
		{name: "simple topic", topic: "orders", want: []string{"orders", "orders.>", "schedules.orders.>"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StreamConfig(tt.topic)
			if got.Name != tt.topic {
				t.Errorf("StreamConfig(%q).Name = %q, want %q", tt.topic, got.Name, tt.topic)
			}
			if !slices.Equal(got.Subjects, tt.want) {
				t.Errorf("StreamConfig(%q).Subjects = %v, want %v", tt.topic, got.Subjects, tt.want)
			}
			if !got.AllowMsgSchedules {
				t.Errorf("StreamConfig(%q).AllowMsgSchedules = false, want true", tt.topic)
			}
			if got.MaxAge != 0 || got.Replicas != 0 {
				t.Errorf("StreamConfig(%q) set MaxAge=%v Replicas=%d, want server defaults", tt.topic, got.MaxAge, got.Replicas)
			}
		})
	}
}

func TestDataSubjects(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  []string
	}{
		{name: "simple topic", topic: "orders", want: []string{"orders", "orders.>"}},
		{name: "underscored topic", topic: "billing_v2", want: []string{"billing_v2", "billing_v2.>"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DataSubjects(tt.topic); !slices.Equal(got, tt.want) {
				t.Errorf("DataSubjects(%q) = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}

func TestScheduleSubject(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		prefix string
	}{
		{name: "simple topic", topic: "orders", prefix: "schedules.orders."},
		{name: "underscored topic", topic: "billing_v2", prefix: "schedules.billing_v2."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ScheduleSubject(tt.topic)
			if !strings.HasPrefix(got, tt.prefix) || len(got) == len(tt.prefix) {
				t.Errorf("ScheduleSubject(%q) = %q, want prefix %q and a unique suffix", tt.topic, got, tt.prefix)
			}
		})
	}

	t.Run("unique across calls", func(t *testing.T) {
		seen := make(map[string]struct{}, 1000)
		for range 1000 {
			s := ScheduleSubject("orders")
			if _, dup := seen[s]; dup {
				t.Fatalf("ScheduleSubject(%q) returned duplicate %q", "orders", s)
			}
			seen[s] = struct{}{}
		}
	})
}

func TestPatchStream(t *testing.T) {
	const topic = "orders"
	tests := []struct {
		name        string
		in          jetstream.StreamConfig
		wantSubject []string
		wantChanged bool
		wantErr     string // substring of the error; empty means no error
	}{
		{
			name: "missing subjects and schedules flag are added",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{topic},
				MaxAge:   time.Hour,
				Replicas: 3,
			},
			wantSubject: []string{topic, topic + ".>", "schedules." + topic + ".>"},
			wantChanged: true,
		},
		{
			name: "already complete is unchanged",
			in: jetstream.StreamConfig{
				Name:              topic,
				Subjects:          []string{topic, topic + ".>", "schedules." + topic + ".>"},
				AllowMsgSchedules: true,
			},
			wantSubject: []string{topic, topic + ".>", "schedules." + topic + ".>"},
			wantChanged: false,
		},
		{
			name: "only the schedules flag is missing",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{topic, topic + ".>", "schedules." + topic + ".>"},
			},
			wantSubject: []string{topic, topic + ".>", "schedules." + topic + ".>"},
			wantChanged: true,
		},
		{
			name: "operator subject is kept and order preserved",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{"audit.>", topic},
			},
			wantSubject: []string{"audit.>", topic, topic + ".>", "schedules." + topic + ".>"},
			wantChanged: true,
		},
		{
			name: "orders.> covers the data subject and the wildcard stays",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{"orders.>"},
			},
			wantSubject: []string{"orders.>", topic, "schedules." + topic + ".>"},
			wantChanged: true,
		},
		{
			name: "> covers every SDK subject",
			in: jetstream.StreamConfig{
				Name:              topic,
				Subjects:          []string{">"},
				AllowMsgSchedules: true,
			},
			wantSubject: []string{">"},
			wantChanged: false,
		},
		{
			name: "orders.* and orders.created are removed under the new orders.>",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{"orders.*", "audit.>", "orders.created"},
			},
			wantSubject: []string{"audit.>", topic, "orders.>", "schedules.orders.>"},
			wantChanged: true,
		},
		{
			name: "orders.* is removed under the new orders.> and schedules are kept",
			in: jetstream.StreamConfig{
				Name:              topic,
				Subjects:          []string{topic, "orders.*"},
				AllowMsgSchedules: true,
			},
			wantSubject: []string{topic, "orders.>", "schedules.orders.>"},
			wantChanged: true,
		},
		{
			name: "schedules.orders.x is removed under the new schedules.orders.>",
			in: jetstream.StreamConfig{
				Name:              topic,
				Subjects:          []string{topic, "schedules.orders.x"},
				AllowMsgSchedules: true,
			},
			wantSubject: []string{topic, "orders.>", "schedules.orders.>"},
			wantChanged: true,
		},
		{
			name: "DenyPurge is rejected",
			in: jetstream.StreamConfig{
				Name:      topic,
				Subjects:  []string{topic},
				DenyPurge: true,
			},
			wantErr: "wire: stream orders: DenyPurge is incompatible with message schedules",
		},
		{
			name: "Discard new is rejected",
			in: jetstream.StreamConfig{
				Name:     topic,
				Subjects: []string{topic},
				Discard:  jetstream.DiscardNew,
			},
			wantErr: "wire: stream orders: Discard policy DiscardNew is incompatible with message schedules",
		},
		{
			name: "mirror is rejected",
			in: jetstream.StreamConfig{
				Name:     topic,
				Mirror:   &jetstream.StreamSource{Name: "upstream"},
				Subjects: []string{topic},
			},
			wantErr: "wire: stream orders: Mirror is incompatible with message schedules",
		},
		{
			name: "sources are rejected",
			in: jetstream.StreamConfig{
				Name:     topic,
				Sources:  []*jetstream.StreamSource{{Name: "upstream"}},
				Subjects: []string{topic},
			},
			wantErr: "wire: stream orders: Sources is incompatible with message schedules",
		},
		{
			name: "AllowMsgCounter is rejected",
			in: jetstream.StreamConfig{
				Name:            topic,
				Subjects:        []string{topic},
				AllowMsgCounter: true,
			},
			wantErr: "wire: stream orders: AllowMsgCounter is incompatible with message schedules",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed, err := PatchStream(tt.in, topic)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("PatchStream(%+v, %q) error = %v, want %q", tt.in, topic, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PatchStream(%+v, %q) error = %v, want nil", tt.in, topic, err)
			}
			if !slices.Equal(got.Subjects, tt.wantSubject) {
				t.Errorf("PatchStream(%+v, %q).Subjects = %v, want %v", tt.in, topic, got.Subjects, tt.wantSubject)
			}
			if changed != tt.wantChanged {
				t.Errorf("PatchStream(%+v, %q) changed = %t, want %t", tt.in, topic, changed, tt.wantChanged)
			}
			if !got.AllowMsgSchedules {
				t.Errorf("PatchStream(%+v, %q).AllowMsgSchedules = false, want true", tt.in, topic)
			}
			if got.Name != tt.in.Name || got.MaxAge != tt.in.MaxAge || got.Replicas != tt.in.Replicas {
				t.Errorf("PatchStream(%+v, %q) changed fields other than subjects and AllowMsgSchedules: got %+v", tt.in, topic, got)
			}
		})
	}
}

func TestCovers(t *testing.T) {
	tests := []struct {
		wide, narrow string
		want         bool
	}{
		{"orders", "orders", true},
		{"orders", "orders.created", false},
		{"orders.created", "orders", false},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders", false},
		{"orders.*", "orders.created.x", false},
		{"orders.*", "orders.>", false},
		{"orders.*", "orders.*", true},
		{"orders.>", "orders", false},
		{"orders.>", "orders.created", true},
		{"orders.>", "orders.created.x", true},
		{"orders.>", "orders.*", true},
		{"orders.>", "orders.>", true},
		{"orders.>", "audit.created", false},
		{">", "orders", true},
		{">", "orders.created.x", true},
		{"*", "orders.created", false},
		{"*.created", "orders.created", true},
		{"*.created", "orders.updated", false},
		{"*.created", "orders.*", false},
		{"orders.created", "orders.*", false},
		{"orders.created", "orders.>", false},
	}
	for _, tt := range tests {
		t.Run(tt.wide+" covers "+tt.narrow, func(t *testing.T) {
			if got := covers(tt.wide, tt.narrow); got != tt.want {
				t.Errorf("covers(%q, %q) = %t, want %t", tt.wide, tt.narrow, got, tt.want)
			}
		})
	}
}

// TestPatchStreamDoesNotMutateInput checks that the input subject slice keeps its
// length and contents when subjects are appended.
func TestPatchStreamDoesNotMutateInput(t *testing.T) {
	// Spare capacity makes an in-place append visible through the original slice.
	subjects := make([]string, 1, 8)
	subjects[0] = "orders"
	in := jetstream.StreamConfig{Name: "orders", Subjects: subjects}

	got, changed, err := PatchStream(in, "orders")
	if err != nil {
		t.Fatalf("PatchStream() error = %v, want nil", err)
	}
	if !changed {
		t.Fatal("PatchStream() changed = false, want true")
	}
	if len(got.Subjects) != 3 {
		t.Fatalf("PatchStream() returned %d subjects, want 3", len(got.Subjects))
	}
	if !slices.Equal(in.Subjects, []string{"orders"}) {
		t.Errorf("input Subjects = %v after PatchStream, want [orders]", in.Subjects)
	}
	if in.AllowMsgSchedules {
		t.Error("input AllowMsgSchedules = true after PatchStream, want false")
	}
}

func TestServerAddrPort(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		wantHost  string
		wantPort  int
	}{
		{name: "no endpoints", endpoints: nil, wantHost: "", wantPort: 0},
		{name: "host and port", endpoints: []string{"127.0.0.1:4223"}, wantHost: "127.0.0.1", wantPort: 4223},
		{name: "nats scheme with port", endpoints: []string{"nats://db.internal:4333"}, wantHost: "db.internal", wantPort: 4333},
		{name: "nats scheme without port", endpoints: []string{"nats://db.internal"}, wantHost: "db.internal", wantPort: 4222},
		{name: "ipv6 with nats scheme", endpoints: []string{"nats://[::1]:4222"}, wantHost: "::1", wantPort: 4222},
		{name: "host without port", endpoints: []string{"localhost"}, wantHost: "localhost", wantPort: 4222},
		{name: "non-numeric port", endpoints: []string{"host:abc"}, wantHost: "host", wantPort: 4222},
		{name: "first endpoint wins", endpoints: []string{"a.local:1", "b.local:2"}, wantHost: "a.local", wantPort: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port := ServerAddrPort(tt.endpoints)
			if host != tt.wantHost || port != tt.wantPort {
				t.Errorf("ServerAddrPort(%q) = (%q, %d), want (%q, %d)", tt.endpoints, host, port, tt.wantHost, tt.wantPort)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	tests := []struct {
		name  string
		token string // server token; empty means no authentication
		opts  func(url string) Options
	}{
		{
			name: "embedded server without auth",
			opts: func(url string) Options {
				return Options{Endpoints: []string{url}, Timeout: 2 * time.Second}
			},
		},
		{
			name:  "embedded server with token",
			token: "s3cret",
			opts: func(url string) Options {
				return Options{Endpoints: []string{url}, Timeout: 2 * time.Second, Token: "s3cret"}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := startServer(t, tt.token)
			opts := tt.opts(ns.ClientURL())

			nc, err := Connect(context.Background(), "wire-test", opts)
			if err != nil {
				t.Fatalf("Connect(%q, %+v): got error %v, want nil", "wire-test", opts, err)
			}
			t.Cleanup(nc.Close)

			if !nc.IsConnected() {
				t.Errorf("Connect(%q, ...) IsConnected() = false, want true", "wire-test")
			}
			if nc.Opts.Name != "wire-test" {
				t.Errorf("Connect(%q, ...) connection name = %q, want %q", "wire-test", nc.Opts.Name, "wire-test")
			}
		})
	}
}

func TestConnectInvalidNKeySeed(t *testing.T) {
	seedFile := filepath.Join(t.TempDir(), "seed.nk")
	if err := os.WriteFile(seedFile, []byte("not a seed"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): got error %v, want nil", seedFile, err)
	}

	tests := []struct {
		name string
		nkey string
	}{
		{name: "seed file with invalid contents", nkey: seedFile},
		{name: "missing seed file", nkey: filepath.Join(t.TempDir(), "absent.nk")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc, err := Connect(context.Background(), "wire-test", Options{
				Endpoints: []string{deadEndpoint(t)},
				Timeout:   time.Second,
				NKey:      tt.nkey,
			})
			if err == nil {
				nc.Close()
				t.Fatal("Connect() with invalid NKey: got error nil, want non-nil")
			}
			if errors.Unwrap(err) == nil {
				t.Errorf("Connect() error = %v, want it to wrap the NKey failure", err)
			}
		})
	}
}

// TestPatchStreamAcceptedByServer checks that a stream the server already holds
// with overlapping subjects can be updated with the patched config.
func TestPatchStreamAcceptedByServer(t *testing.T) {
	t.Parallel()

	ns := startServer(t, "")
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v, want nil", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v, want nil", err)
	}
	ctx := context.Background()

	tests := []struct {
		name     string
		topic    string
		subjects []string
	}{
		{name: "orders.* overlaps the data subject", topic: "orders", subjects: []string{"orders", "orders.*"}},
		{name: "shipments.created is replaced by shipments.>", topic: "shipments", subjects: []string{"shipments.created"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := tt.topic
			if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: topic, Subjects: tt.subjects}); err != nil {
				t.Fatalf("CreateStream(%v) error = %v, want nil", tt.subjects, err)
			}
			s, err := js.Stream(ctx, topic)
			if err != nil {
				t.Fatalf("Stream() error = %v, want nil", err)
			}
			patched, changed, err := PatchStream(s.CachedInfo().Config, topic)
			if err != nil {
				t.Fatalf("PatchStream() error = %v, want nil", err)
			}
			if !changed {
				t.Fatal("PatchStream() changed = false, want true")
			}
			if _, err := js.UpdateStream(ctx, patched); err != nil {
				t.Errorf("UpdateStream(%v) error = %v, want nil", patched.Subjects, err)
			}
		})
	}
}

func TestConnectCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nc, err := Connect(ctx, "wire-test", Options{Endpoints: []string{deadEndpoint(t)}})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Connect() error = %v, want %v", err, context.Canceled)
	}
	if nc != nil {
		nc.Close()
		t.Error("Connect() returned a connection with a cancelled context")
	}
}

// regression: a connect error must not carry the userinfo of an endpoint.
func TestConnectErrorHasNoCredentials(t *testing.T) {
	t.Parallel()

	nc, err := Connect(context.Background(), "wire-test", Options{
		Endpoints: []string{"nats://user:pass@127.0.0.1:notaport"},
		Timeout:   time.Second,
	})
	if nc != nil {
		nc.Close()
	}
	if err == nil {
		t.Fatal("Connect() error = nil, want non-nil for a malformed endpoint")
	}
	if strings.Contains(err.Error(), "user:pass") {
		t.Errorf("Connect() error = %q, want no credentials", err)
	}
}

func TestRedactEndpoints(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		want      string
	}{
		{name: "userinfo is removed", endpoints: []string{"nats://user:pass@db.internal:4222"}, want: "nats://db.internal:4222"},
		{name: "plain endpoint is kept", endpoints: []string{"nats://db.internal:4222"}, want: "nats://db.internal:4222"},
		{name: "host and port get the scheme", endpoints: []string{"127.0.0.1:4222"}, want: "nats://127.0.0.1:4222"},
		{name: "userinfo without scheme is removed", endpoints: []string{"user:pass@127.0.0.1:4222"}, want: "nats://127.0.0.1:4222"},
		{name: "unparseable endpoint is hidden", endpoints: []string{"nats://user:pass@%zz"}, want: "<invalid endpoint>"},
		{name: "several endpoints are joined", endpoints: []string{"nats://a:1", "nats://u:p@b:2"}, want: "nats://a:1,nats://b:2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactEndpoints(tt.endpoints); got != tt.want {
				t.Errorf("redactEndpoints(%q) = %q, want %q", tt.endpoints, got, tt.want)
			}
		})
	}
}

func TestRedactUserinfo(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
	}{
		{
			name: "parse error text",
			msg:  `parse "nats://user:pass@127.0.0.1:notaport": invalid port`,
			want: `parse "nats://127.0.0.1:notaport": invalid port`,
		},
		{name: "no userinfo is unchanged", msg: "dial tcp 127.0.0.1:4222: refused", want: "dial tcp 127.0.0.1:4222: refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactUserinfo(tt.msg); got != tt.want {
				t.Errorf("redactUserinfo(%q) = %q, want %q", tt.msg, got, tt.want)
			}
		})
	}
}

func TestDialTimeout(t *testing.T) {
	tests := []struct {
		name      string
		timeout   time.Duration
		endpoints int
		deadline  time.Duration // from now; zero means no deadline
		wantMin   time.Duration
		wantMax   time.Duration
	}{
		{name: "zero timeout uses the default", timeout: 0, endpoints: 1, wantMin: nats.DefaultTimeout, wantMax: nats.DefaultTimeout},
		{name: "explicit timeout is kept", timeout: 3 * time.Second, endpoints: 1, wantMin: 3 * time.Second, wantMax: 3 * time.Second},
		{name: "deadline caps the timeout per endpoint", endpoints: 2, deadline: time.Second, wantMin: 400 * time.Millisecond, wantMax: 500 * time.Millisecond},
		{name: "deadline beyond the timeout keeps the timeout", timeout: time.Second, endpoints: 1, deadline: time.Minute, wantMin: time.Second, wantMax: time.Second},
		{name: "expired deadline has a floor of 1ms", endpoints: 1, deadline: -time.Second, wantMin: time.Millisecond, wantMax: time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.deadline != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(tt.deadline))
				defer cancel()
			}
			eps := make([]string, tt.endpoints)
			got := dialTimeout(ctx, Options{Endpoints: eps, Timeout: tt.timeout})
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("dialTimeout() = %v, want between %v and %v", got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestDrain(t *testing.T) {
	t.Parallel()

	t.Run("flushes and closes", func(t *testing.T) {
		t.Parallel()

		ns := startServer(t, "")
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatalf("nats.Connect() error = %v, want nil", err)
		}

		if err := Drain(context.Background(), nc); err != nil {
			t.Fatalf("Drain() error = %v, want nil", err)
		}
		if !nc.IsClosed() {
			t.Error("Drain() returned with the connection open, want it closed")
		}
	})

	t.Run("already closed is not an error", func(t *testing.T) {
		t.Parallel()

		ns := startServer(t, "")
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatalf("nats.Connect() error = %v, want nil", err)
		}
		nc.Close()

		if err := Drain(context.Background(), nc); err != nil {
			t.Errorf("Drain() on a closed connection error = %v, want nil", err)
		}
	})

	t.Run("deadline closes the connection", func(t *testing.T) {
		t.Parallel()

		ns := startServer(t, "")
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatalf("nats.Connect() error = %v, want nil", err)
		}

		// The handler blocks, so the drain cannot finish before the deadline.
		release := make(chan struct{})
		entered := make(chan struct{}, 1)
		t.Cleanup(func() { close(release) })
		if _, err := nc.Subscribe("blocked", func(*nats.Msg) {
			entered <- struct{}{}
			<-release
		}); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if err := nc.Publish("blocked", []byte("hold")); err != nil {
			t.Fatalf("Publish() error = %v, want nil", err)
		}
		<-entered

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err := Drain(ctx, nc); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Drain() error = %v, want %v", err, context.DeadlineExceeded)
		}
		if !nc.IsClosed() {
			t.Error("Drain() returned with the connection open, want it closed")
		}
	})
}
