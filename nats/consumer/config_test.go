package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConfig_SetDefaults(t *testing.T) {
	t.Parallel()

	c := Config{}.SetDefaults()
	if c.Timeout != DefaultTimeout {
		t.Fatalf("Timeout = %v, want %v", c.Timeout, DefaultTimeout)
	}
	if c.MaxRetryAttempts != DefaultMaxRetryAttempts {
		t.Fatalf("MaxRetryAttempts = %d, want %d", c.MaxRetryAttempts, DefaultMaxRetryAttempts)
	}
	if c.MaxConcurrent != DefaultMaxConcurrent {
		t.Fatalf("MaxConcurrent = %d, want %d", c.MaxConcurrent, DefaultMaxConcurrent)
	}
	if len(c.Tags) != 1 || c.Tags[0] != DefaultTags {
		t.Fatalf("Tags = %v, want [%s]", c.Tags, DefaultTags)
	}
	if c.ConsumeFromWhere != ConsumeFromLast {
		t.Fatalf("ConsumeFromWhere = %q, want %q", c.ConsumeFromWhere, ConsumeFromLast)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	t.Parallel()

	c := Config{
		Timeout:          30 * time.Second,
		MaxRetryAttempts: 3,
		MaxConcurrent:    5,
		Tags:             []string{"tag1", "tag2"},
		ConsumeFromWhere: ConsumeFromFirst,
		ConsumeTimestamp: "2026-01-01T00:00:00Z",
		RetryBackoff:     time.Second,
	}.SetDefaults()

	if c.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v, want 30s", c.Timeout)
	}
	if c.MaxRetryAttempts != 3 {
		t.Fatalf("MaxRetryAttempts = %d, want 3", c.MaxRetryAttempts)
	}
	if c.MaxConcurrent != 5 {
		t.Fatalf("MaxConcurrent = %d, want 5", c.MaxConcurrent)
	}
	if len(c.Tags) != 2 || c.Tags[0] != "tag1" {
		t.Fatalf("Tags = %v, want [tag1 tag2]", c.Tags)
	}
	if c.ConsumeFromWhere != ConsumeFromFirst {
		t.Fatalf("ConsumeFromWhere = %q, want %q", c.ConsumeFromWhere, ConsumeFromFirst)
	}
	if c.RetryBackoff != time.Second {
		t.Fatalf("RetryBackoff = %v, want 1s", c.RetryBackoff)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()

	c := Config{
		Endpoints:        []string{"localhost:4222"},
		Topic:            "test-topic",
		Group:            "test-group",
		ConsumeFromWhere: ConsumeFromTimestamp,
		ConsumeTimestamp: "2026-01-01T00:00:00Z",
		RetryBackoff:     3 * time.Second,
	}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()

	valid := Config{Endpoints: []string{"localhost:4222"}, Topic: "t", Group: "g"}
	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"empty endpoints", Config{Topic: "t", Group: "g"}, ErrEndpointsRequired},
		{"nil endpoints", Config{Endpoints: nil, Topic: "t", Group: "g"}, ErrEndpointsRequired},
		{"empty topic", Config{Endpoints: []string{"localhost:4222"}, Group: "g"}, ErrTopicRequired},
		{"empty group", Config{Endpoints: []string{"localhost:4222"}, Topic: "t"}, ErrGroupRequired},
		{"invalid consume from where", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: "weird"}, ErrInvalidConsumeFromWhere},
		{"timestamp missing when required", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: ConsumeFromTimestamp}, ErrInvalidConsumeTimestamp},
		{"timestamp malformed", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: ConsumeFromTimestamp, ConsumeTimestamp: "not-a-time"}, ErrInvalidConsumeTimestamp},
		{"negative retry backoff", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", RetryBackoff: -time.Second}, ErrNegativeRetryBackoff},
		{"topic with dots", Config{Endpoints: valid.Endpoints, Topic: "has.dots", Group: "g"}, ErrInvalidTopic},
		{"group with dots", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "has.dots"}, ErrInvalidGroup},
		{"tag with dots", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", Tags: []string{"has.dots"}}, ErrInvalidTag},
		{"empty tag in tags", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", Tags: []string{""}}, ErrEmptyTag},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.SetDefaults().Validate()
			if err == nil {
				t.Fatal("Validate: expected error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want it to wrap %v", err, tc.want)
			}
		})
	}
}

func TestConfig_Validate_Aggregates(t *testing.T) {
	t.Parallel()

	err := Config{ConsumeFromWhere: "weird"}.SetDefaults().Validate()
	if err == nil {
		t.Fatal("Validate: expected error")
	}
	if !errors.Is(err, ErrEndpointsRequired) {
		t.Fatalf("Validate() = %v, want endpoints error", err)
	}
	if !errors.Is(err, ErrTopicRequired) {
		t.Fatalf("Validate() = %v, want topic error", err)
	}
	if !errors.Is(err, ErrGroupRequired) {
		t.Fatalf("Validate() = %v, want group error", err)
	}
	if !errors.Is(err, ErrInvalidConsumeFromWhere) {
		t.Fatalf("Validate() = %v, want consume from where error", err)
	}
}

func TestConfig_BuildFilterSubjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		topic string
		tags  []string
		want  []string
	}{
		{
			name:  "default wildcard",
			topic: "orders",
			tags:  []string{"*"},
			want:  nil,
		},
		{
			name:  "empty tags",
			topic: "orders",
			tags:  nil,
			want:  nil,
		},
		{
			name:  "single tag",
			topic: "orders",
			tags:  []string{"created"},
			want:  []string{"orders.created"},
		},
		{
			name:  "multiple tags",
			topic: "orders",
			tags:  []string{"created", "updated"},
			want:  []string{"orders.created", "orders.updated"},
		},
		{
			name:  "topic as tag",
			topic: "orders",
			tags:  []string{"orders"},
			want:  []string{"orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := Config{Topic: tt.topic, Tags: tt.tags}
			got := c.buildFilterSubjects()
			if len(got) != len(tt.want) {
				t.Fatalf("buildFilterSubjects() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("buildFilterSubjects()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestConfig_BuildConsumerConfig(t *testing.T) {
	t.Parallel()

	c := Config{
		Topic:            "orders",
		Group:            "processor",
		Tags:             []string{"created", "updated"},
		Timeout:          30 * time.Second,
		MaxRetryAttempts: 3,
		ConsumeFromWhere: ConsumeFromFirst,
	}.SetDefaults()

	jsCfg := c.buildConsumerConfig("app_orders_processor")
	if jsCfg.Durable != "app_orders_processor" {
		t.Errorf("Durable = %q, want app_orders_processor", jsCfg.Durable)
	}
	if jsCfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("DeliverPolicy = %v, want %v", jsCfg.DeliverPolicy, jetstream.DeliverAllPolicy)
	}
	if len(jsCfg.FilterSubjects) != 2 {
		t.Errorf("FilterSubjects len = %d, want 2", len(jsCfg.FilterSubjects))
	}
	if jsCfg.AckWait != 30*time.Second {
		t.Errorf("AckWait = %v, want 30s", jsCfg.AckWait)
	}
	if jsCfg.MaxDeliver != 3 {
		t.Errorf("MaxDeliver = %d, want 3", jsCfg.MaxDeliver)
	}

	// Single tag sets FilterSubject
	singleTagCfg := Config{
		Topic: "orders",
		Tags:  []string{"created"},
	}.SetDefaults()
	jsCfgSingle := singleTagCfg.buildConsumerConfig("durable")
	if jsCfgSingle.FilterSubject != "orders.created" {
		t.Errorf("FilterSubject = %q, want orders.created", jsCfgSingle.FilterSubject)
	}

	// Timestamp delivery
	tsCfg := Config{
		Topic:            "orders",
		ConsumeFromWhere: ConsumeFromTimestamp,
		ConsumeTimestamp: "2026-01-01T00:00:00Z",
	}.SetDefaults()
	jsCfgTs := tsCfg.buildConsumerConfig("durable")
	if jsCfgTs.DeliverPolicy != jetstream.DeliverByStartTimePolicy {
		t.Errorf("DeliverPolicy = %v, want DeliverByStartTimePolicy", jsCfgTs.DeliverPolicy)
	}
	if jsCfgTs.OptStartTime == nil {
		t.Error("OptStartTime is nil")
	}
}
