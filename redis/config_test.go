package redis

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	c := Config{Host: "localhost"}.SetDefaults()
	if c.Mode != ModeStandalone {
		t.Fatalf("Mode = %v, want standalone", c.Mode)
	}
	if c.Port != defaultPort {
		t.Fatalf("Port = %d, want %d", c.Port, defaultPort)
	}
	if c.ConnectTimeout != defaultConnectTimeout {
		t.Fatalf("ConnectTimeout = %v, want %v", c.ConnectTimeout, defaultConnectTimeout)
	}
	if len(c.Addrs) != 1 || c.Addrs[0] != "localhost:6379" {
		t.Fatalf("Addrs = %v, want [localhost:6379]", c.Addrs)
	}
}

func TestConfig_SetDefaults_Sentinel(t *testing.T) {
	c := Config{
		Mode:       " SENTINEL ",
		MasterName: "mymaster",
		Addrs:      []string{"sentinel.service.consul"},
	}.SetDefaults()
	if c.Mode != ModeSentinel {
		t.Fatalf("Mode = %v, want sentinel", c.Mode)
	}
	if len(c.Addrs) != 1 || c.Addrs[0] != "sentinel.service.consul:26379" {
		t.Fatalf("Addrs = %v, want [sentinel.service.consul:26379]", c.Addrs)
	}
}

func TestConfig_SetDefaults_Cluster(t *testing.T) {
	c := Config{
		Mode:  "CLUSTER",
		Addrs: []string{"node1", "node2:7001"},
	}.SetDefaults()
	if c.Mode != ModeCluster {
		t.Fatalf("Mode = %v, want cluster", c.Mode)
	}
	if c.MaxRedirects != defaultMaxRedirects {
		t.Fatalf("MaxRedirects = %d, want %d", c.MaxRedirects, defaultMaxRedirects)
	}
	if len(c.Addrs) != 2 || c.Addrs[0] != "node1:6379" || c.Addrs[1] != "node2:7001" {
		t.Fatalf("Addrs = %v, want [node1:6379 node2:7001]", c.Addrs)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	c := Config{
		Host:           "localhost",
		Port:           6380,
		ConnectTimeout: 3 * time.Second,
	}.SetDefaults()
	if c.Port != 6380 {
		t.Fatalf("Port = %d, want 6380", c.Port)
	}
	if c.ConnectTimeout != 3*time.Second {
		t.Fatalf("ConnectTimeout = %v, want 3s", c.ConnectTimeout)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	// Standalone
	c := Config{Host: "localhost", Port: 6379}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate standalone: %v", err)
	}

	// Sentinel
	c = Config{
		Mode:       ModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"sentinel-1:26379", "sentinel-2:26379"},
	}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate sentinel: %v", err)
	}

	// Cluster
	c = Config{
		Mode:  ModeCluster,
		Addrs: []string{"node-1:6379", "node-2:6379"},
		DB:    0,
	}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate cluster: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing host standalone", Config{Port: 6379}, "host or addrs is required"},
		{"port too low", Config{Host: "h", Port: -1}, "out of range"},
		{"port too high", Config{Host: "h", Port: 65536}, "out of range"},
		{"sentinel missing master name", Config{Mode: ModeSentinel, Addrs: []string{"s:26379"}}, "master_name is required in sentinel mode"},
		{"sentinel missing addrs", Config{Mode: ModeSentinel, MasterName: "m"}, "at least one sentinel address in addrs is required"},
		{"cluster missing addrs", Config{Mode: ModeCluster}, "at least one cluster seed address in addrs is required"},
		{"cluster non zero db", Config{Mode: ModeCluster, Addrs: []string{"n:6379"}, DB: 1}, "cluster mode only supports DB 0"},
		{"unknown mode", Config{Mode: "custom", Host: "h"}, "unknown mode"},
		{"negative db", Config{Host: "h", Port: 6379, DB: -1}, "DB must not be negative"},
		{"negative dial timeout", Config{Host: "h", Port: 6379, DialTimeout: -1}, "DialTimeout must not be negative"},
		{"negative pool size", Config{Host: "h", Port: 6379, PoolSize: -1}, "PoolSize must not be negative"},
		{"min idle over pool", Config{Host: "h", Port: 6379, PoolSize: 5, MinIdleConns: 6}, "must not exceed PoolSize"},
		{"max idle over pool", Config{Host: "h", Port: 6379, PoolSize: 5, MaxIdleConns: 6}, "must not exceed PoolSize"},
		{"min idle over max idle", Config{Host: "h", Port: 6379, PoolSize: 10, MinIdleConns: 6, MaxIdleConns: 5}, "must not exceed MaxIdleConns"},
		{"tls cert without key", Config{Host: "h", Port: 6379, TLSCert: "cert"}, "TLSCert and TLSKey must both be set"},
		{"tls certs without tls enabled", Config{Host: "h", Port: 6379, TLSEnabled: false, TLSCACert: "ca.pem"}, "require TLSEnabled to be true"},
		{"negative conn max lifetime", Config{Host: "h", Port: 6379, ConnMaxLifetime: -1}, "ConnMaxLifetime must not be negative"},
		{"negative conn max idle time", Config{Host: "h", Port: 6379, ConnMaxIdleTime: -1}, "ConnMaxIdleTime must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.SetDefaults().Validate()
			if err == nil {
				t.Fatal("Validate: expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}
