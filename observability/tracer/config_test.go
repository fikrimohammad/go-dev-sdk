package tracer

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestConfig_SetDefaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}.SetDefaults()

		if cfg.Endpoint != "localhost:4317" {
			t.Errorf("SetDefaults() Endpoint = %q, want %q", cfg.Endpoint, "localhost:4317")
		}
		if cfg.ExportTimeout != 5*time.Second {
			t.Errorf("SetDefaults() ExportTimeout = %v, want %v", cfg.ExportTimeout, 5*time.Second)
		}
		if cfg.BatchTimeout != 5*time.Second {
			t.Errorf("SetDefaults() BatchTimeout = %v, want %v", cfg.BatchTimeout, 5*time.Second)
		}
		if cfg.MaxQueueSize != 2048 {
			t.Errorf("SetDefaults() MaxQueueSize = %d, want 2048", cfg.MaxQueueSize)
		}
		if cfg.MaxExportBatchSize != 512 {
			t.Errorf("SetDefaults() MaxExportBatchSize = %d, want 512", cfg.MaxExportBatchSize)
		}
		if cfg.Sampler != nil {
			t.Errorf("SetDefaults() Sampler = %v, want nil", cfg.Sampler)
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		sampler := sdktrace.NeverSample()
		cfg := Config{
			Endpoint:           "collector:4318",
			ExportTimeout:      3 * time.Second,
			Headers:            map[string]string{"auth": "x"},
			GlobalKV:           map[string]any{"cloud.region": "eu"},
			Sampler:            sampler,
			BatchTimeout:       2 * time.Second,
			MaxQueueSize:       100,
			MaxExportBatchSize: 10,
		}.SetDefaults()
		if cfg.Endpoint != "collector:4318" {
			t.Errorf("SetDefaults() Endpoint = %q, want %q", cfg.Endpoint, "collector:4318")
		}
		if cfg.ExportTimeout != 3*time.Second {
			t.Errorf("SetDefaults() ExportTimeout = %v, want %v", cfg.ExportTimeout, 3*time.Second)
		}
		if want := map[string]string{"auth": "x"}; !maps.Equal(cfg.Headers, want) {
			t.Errorf("SetDefaults() Headers = %v, want %v", cfg.Headers, want)
		}
		if want := map[string]any{"cloud.region": "eu"}; !maps.Equal(cfg.GlobalKV, want) {
			t.Errorf("SetDefaults() GlobalKV = %v, want %v", cfg.GlobalKV, want)
		}
		if cfg.Sampler != sampler {
			t.Errorf("SetDefaults() Sampler = %v, want %v", cfg.Sampler, sampler)
		}
		if cfg.BatchTimeout != 2*time.Second {
			t.Errorf("SetDefaults() BatchTimeout = %v, want %v", cfg.BatchTimeout, 2*time.Second)
		}
		if cfg.MaxQueueSize != 100 {
			t.Errorf("SetDefaults() MaxQueueSize = %d, want 100", cfg.MaxQueueSize)
		}
		if cfg.MaxExportBatchSize != 10 {
			t.Errorf("SetDefaults() MaxExportBatchSize = %d, want 10", cfg.MaxExportBatchSize)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	t.Run("a defaulted Config validates", func(t *testing.T) {
		cfg := Config{}.SetDefaults()
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "non-positive export timeout is rejected", cfg: Config{ExportTimeout: -time.Second}},
		{name: "non-positive batch settings are rejected", cfg: Config{BatchTimeout: -time.Second, MaxQueueSize: -1, MaxExportBatchSize: -1}},
		{name: "max export batch size over the queue size is rejected", cfg: Config{MaxQueueSize: 10, MaxExportBatchSize: 11}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg.SetDefaults()
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate(%+v) = nil, want non-nil", cfg)
			}
		})
	}
}

func TestConfig(t *testing.T) {
	t.Run("the zero-value Config uses TLS", func(t *testing.T) {
		if got := (Config{}).Insecure; got {
			t.Errorf("Config{}.Insecure = %v, want false", got)
		}
	})

	tests := []struct {
		name     string
		insecure bool
		wantHead string // prefix of the first bytes the exporter sends
	}{
		{name: "Insecure false starts a TLS handshake", insecure: false, wantHead: "\x16\x03"},
		{name: "Insecure true starts a plaintext HTTP/2 preface", insecure: true, wantHead: "PRI * HTTP/2.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("net.Listen() error = %v, want nil", err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			first := listenFirstBytes(t, ln)

			c, err := New(context.Background(), testInfo(), Config{
				Endpoint:      ln.Addr().String(),
				ExportTimeout: 500 * time.Millisecond,
				Insecure:      tt.insecure,
			})
			if err != nil {
				t.Fatalf("New(Insecure=%t) error = %v, want nil", tt.insecure, err)
			}
			_, span := c.Tracer("probe").Start(context.Background(), "op")
			span.End()
			stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			// The fake collector never answers, so Stop is expected to fail.
			_ = c.Stop(stopCtx)

			select {
			case b := <-first:
				if !bytes.HasPrefix(b, []byte(tt.wantHead)) {
					t.Errorf("New(Insecure=%t) first bytes = %q, want prefix %q", tt.insecure, b, tt.wantHead)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("New(Insecure=%t) exporter did not connect within 5s", tt.insecure)
			}
		})
	}
}

// listenFirstBytes accepts one connection on ln in the background and sends
// up to 14 received bytes on the returned channel. The channel is buffered, so
// the goroutine finishes after its single send. Closing ln ends it early.
func listenFirstBytes(t *testing.T, ln net.Listener) <-chan []byte {
	t.Helper()
	first := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			first <- nil
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, len("PRI * HTTP/2.0"))
		n, _ := io.ReadFull(conn, buf)
		first <- buf[:n]
	}()
	return first
}
