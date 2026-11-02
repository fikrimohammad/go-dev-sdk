package wire

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

// startServer runs an in-process JetStream server on a random port and stops it
// when the test ends. A non-empty token enables token authentication.
func startServer(t *testing.T, token string) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:          -1,
		JetStream:     true,
		StoreDir:      t.TempDir(),
		Authorization: token,
	})
	if err != nil {
		t.Fatalf("NewServer(): got error %v, want nil", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready for connections within 10s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns
}

func TestStreamConfig(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  []string
	}{
		{name: "simple topic", topic: "orders", want: []string{"orders", "orders.>", "schedules.orders.>"}},
		{name: "nested topic", topic: "billing_v2", want: []string{"billing_v2", "billing_v2.>", "schedules.billing_v2.>"}},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := PatchStream(tt.in, topic)
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

// TestPatchStreamKeepsOperatorFields checks that operator-set fields survive a
// patch, including the MaxAge and Replicas values the producer must not reset.
func TestPatchStreamKeepsOperatorFields(t *testing.T) {
	in := jetstream.StreamConfig{
		Name:     "orders",
		Subjects: []string{"orders"},
		MaxAge:   time.Hour,
		Replicas: 3,
	}
	got, _ := PatchStream(in, "orders")
	if got.MaxAge != time.Hour {
		t.Errorf("PatchStream(...).MaxAge = %v, want %v", got.MaxAge, time.Hour)
	}
	if got.Replicas != 3 {
		t.Errorf("PatchStream(...).Replicas = %d, want 3", got.Replicas)
	}
}

// TestPatchStreamDoesNotMutateInput checks that the input subject slice keeps its
// length and contents when subjects are appended.
func TestPatchStreamDoesNotMutateInput(t *testing.T) {
	// Spare capacity makes an in-place append visible through the original slice.
	subjects := make([]string, 1, 8)
	subjects[0] = "orders"
	in := jetstream.StreamConfig{Name: "orders", Subjects: subjects}

	got, changed := PatchStream(in, "orders")
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

			nc, err := Connect("wire-test", opts)
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
			nc, err := Connect("wire-test", Options{
				Endpoints: []string{"nats://127.0.0.1:1"},
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
