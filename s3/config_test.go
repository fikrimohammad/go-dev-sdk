package s3

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_SetDefaults(t *testing.T) {
	c := Config{}.SetDefaults()
	if c.PresignDefaultExpiry != 15*time.Minute {
		t.Fatalf("PresignDefaultExpiry = %v, want 15m", c.PresignDefaultExpiry)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	c := Config{PresignDefaultExpiry: 5 * time.Minute}.SetDefaults()
	if c.PresignDefaultExpiry != 5*time.Minute {
		t.Fatalf("PresignDefaultExpiry = %v, want 5m", c.PresignDefaultExpiry)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	c := Config{Region: "us-east-1"}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_ValidWithAllSettings(t *testing.T) {
	c := Config{
		Region:                   "us-east-1",
		Endpoint:                 "http://localhost:9000",
		AccessKeyID:              "key",
		SecretAccessKey:          "secret",
		UploadPartSizeBytes:      5 * 1024 * 1024,
		UploadMultipartThreshold: 16 * 1024 * 1024,
		TransferConcurrency:      3,
		PresignDefaultExpiry:     time.Minute,
	}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing region", Config{}, "region is required"},
		{"creds only key", Config{Region: "r", AccessKeyID: "k"}, "both be set"},
		{"creds only secret", Config{Region: "r", SecretAccessKey: "s"}, "both be set"},
		{"endpoint no scheme", Config{Region: "r", Endpoint: "localhost:9000"}, "must use http or https"},
		{"negative part size", Config{Region: "r", UploadPartSizeBytes: -1}, "must not be negative"},
		{"part size below minimum", Config{Region: "r", UploadPartSizeBytes: 1024}, "at least"},
		{"negative multipart threshold", Config{Region: "r", UploadMultipartThreshold: -1}, "must not be negative"},
		{"negative transfer concurrency", Config{Region: "r", TransferConcurrency: -1}, "must not be negative"},
		{"negative presign expiry", Config{Region: "r", PresignDefaultExpiry: -time.Second}, "must be positive"},
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

func TestConfig_Validate_Aggregates(t *testing.T) {
	err := Config{AccessKeyID: "k"}.SetDefaults().Validate()
	if err == nil {
		t.Fatal("Validate: expected error")
	}
	if !strings.Contains(err.Error(), "region is required") {
		t.Fatalf("Validate() = %q, want missing region error", err.Error())
	}
	if !strings.Contains(err.Error(), "both be set") {
		t.Fatalf("Validate() = %q, want credential mismatch error", err.Error())
	}
}
