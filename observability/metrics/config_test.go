package metrics

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	t.Run("an empty Config gets sane defaults", func(t *testing.T) {
		cfg := Config{}.SetDefaults()

		if cfg.Endpoint != "localhost:4317" {
			t.Errorf("SetDefaults() Endpoint = %q, want %q", cfg.Endpoint, "localhost:4317")
		}
		if cfg.Timeout != 10*time.Second {
			t.Errorf("SetDefaults() Timeout = %v, want %v", cfg.Timeout, 10*time.Second)
		}
		if cfg.ExportInterval != 10*time.Second {
			t.Errorf("SetDefaults() ExportInterval = %v, want %v", cfg.ExportInterval, 10*time.Second)
		}
		if cfg.Scope != DefaultInstrumentationScope {
			t.Errorf("SetDefaults() Scope = %q, want %q", cfg.Scope, DefaultInstrumentationScope)
		}
		if cfg.HistogramMaxBuckets != 160 {
			t.Errorf("SetDefaults() HistogramMaxBuckets = %d, want 160", cfg.HistogramMaxBuckets)
		}
		if cfg.HistogramMaxScale != 20 {
			t.Errorf("SetDefaults() HistogramMaxScale = %d, want 20", cfg.HistogramMaxScale)
		}
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		cfg := Config{
			Endpoint:       "collector:4318",
			Timeout:        2 * time.Second,
			ExportInterval: 1 * time.Minute,
			Scope:          "custom.scope",
			Headers:        map[string]string{"authorization": "Bearer x"},
		}.SetDefaults()
		if cfg.Endpoint != "collector:4318" {
			t.Errorf("SetDefaults() Endpoint = %q, want %q", cfg.Endpoint, "collector:4318")
		}
		if cfg.Timeout != 2*time.Second {
			t.Errorf("SetDefaults() Timeout = %v, want %v", cfg.Timeout, 2*time.Second)
		}
		if cfg.ExportInterval != 1*time.Minute {
			t.Errorf("SetDefaults() ExportInterval = %v, want %v", cfg.ExportInterval, 1*time.Minute)
		}
		if cfg.Scope != "custom.scope" {
			t.Errorf("SetDefaults() Scope = %q, want %q", cfg.Scope, "custom.scope")
		}
		if want := map[string]string{"authorization": "Bearer x"}; !maps.Equal(cfg.Headers, want) {
			t.Errorf("SetDefaults() Headers = %v, want %v", cfg.Headers, want)
		}
	})

	t.Run("histogram tuning values are preserved", func(t *testing.T) {
		cfg := Config{HistogramMaxBuckets: 256, HistogramMaxScale: 15}.SetDefaults()
		if cfg.HistogramMaxBuckets != 256 {
			t.Errorf("SetDefaults() HistogramMaxBuckets = %d, want 256", cfg.HistogramMaxBuckets)
		}
		if cfg.HistogramMaxScale != 15 {
			t.Errorf("SetDefaults() HistogramMaxScale = %d, want 15", cfg.HistogramMaxScale)
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "a defaulted Config validates", cfg: Config{}},
		{name: "non-positive timeout is rejected", cfg: Config{Timeout: -time.Second}, wantErr: true},
		{name: "non-positive export interval is rejected", cfg: Config{ExportInterval: -time.Second}, wantErr: true},
		{name: "negative histogram max buckets is rejected", cfg: Config{HistogramMaxBuckets: -1}, wantErr: true},
		{name: "histogram max buckets over the cap is rejected", cfg: Config{HistogramMaxBuckets: maxHistogramMaxBuckets + 1}, wantErr: true},
		{name: "histogram max buckets at the cap validates", cfg: Config{HistogramMaxBuckets: maxHistogramMaxBuckets}},
		{name: "histogram max scale above range is rejected", cfg: Config{HistogramMaxScale: 21}, wantErr: true},
		{name: "histogram max scale below range is rejected", cfg: Config{HistogramMaxScale: -11}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg.SetDefaults()
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%+v) = %v, want error: %t", cfg, err, tt.wantErr)
			}
		})
	}

	t.Run("multiple invalid fields are aggregated", func(t *testing.T) {
		cfg := Config{Timeout: -time.Second, ExportInterval: -time.Second, HistogramMaxBuckets: -1}.SetDefaults()
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() = nil, want error")
		}
		for _, want := range []string{"timeout", "export_interval", "histogram_max_buckets"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Validate() = %q, want it to contain %q", err, want)
			}
		}
	})
}

func TestConfig(t *testing.T) {
	t.Run("zero-value Config uses TLS", func(t *testing.T) {
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
				Endpoint: ln.Addr().String(),
				Timeout:  500 * time.Millisecond,
				Insecure: tt.insecure,
			})
			if err != nil {
				t.Fatalf("New(Insecure=%t) error = %v, want nil", tt.insecure, err)
			}
			if err := c.Add(context.Background(), Instrument{Name: "probe"}, 1, nil); err != nil {
				t.Fatalf("Add(probe, 1) error = %v, want nil", err)
			}
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
