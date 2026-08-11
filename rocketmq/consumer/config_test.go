package consumer

import (
	"errors"
	"testing"
	"time"

	rconsumer "github.com/apache/rocketmq-client-go/v2/consumer"
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
	if c.ConsumeModel != ConsumeModelClustering {
		t.Fatalf("ConsumeModel = %q, want %q", c.ConsumeModel, ConsumeModelClustering)
	}
	if c.ConsumeFromWhere != ConsumeFromLast {
		t.Fatalf("ConsumeFromWhere = %q, want %q", c.ConsumeFromWhere, ConsumeFromLast)
	}
}

func TestConfig_SetDefaults_Explicit(t *testing.T) {
	t.Parallel()

	c := Config{
		Timeout:                   30 * time.Second,
		MaxRetryAttempts:          3,
		MaxConcurrent:             5,
		Tags:                      []string{"tag1", "tag2"},
		ConsumeModel:              ConsumeModelBroadcasting,
		ConsumeFromWhere:          ConsumeFromFirst,
		ConsumeOrderly:            true,
		ConsumeTimestamp:          "2026-01-01T00:00:00Z",
		MaxCachedMessagesPerQueue: 1000,
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
	if c.ConsumeModel != ConsumeModelBroadcasting {
		t.Fatalf("ConsumeModel = %q, want %q", c.ConsumeModel, ConsumeModelBroadcasting)
	}
	if c.ConsumeFromWhere != ConsumeFromFirst {
		t.Fatalf("ConsumeFromWhere = %q, want %q", c.ConsumeFromWhere, ConsumeFromFirst)
	}
	if !c.ConsumeOrderly {
		t.Fatal("ConsumeOrderly = false, want true")
	}
	if c.MaxCachedMessagesPerQueue != 1000 {
		t.Fatalf("MaxCachedMessagesPerQueue = %d, want 1000", c.MaxCachedMessagesPerQueue)
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()

	c := Config{
		Endpoints:                  []string{"localhost:9876"},
		Topic:                      "test-topic",
		Group:                      "test-group",
		ConsumeFromWhere:           ConsumeFromTimestamp,
		ConsumeTimestamp:           "2026-01-01T00:00:00Z",
		ConsumeMessageBatchMaxSize: 32,
		MaxCachedMessagesPerQueue:  1000,
		MaxCachedMessagesPerTopic:  10000,
		RetryBackoff:               3 * time.Second,
	}.SetDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()

	valid := Config{Endpoints: []string{"localhost:9876"}, Topic: "t", Group: "g"}
	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"empty endpoints", Config{Topic: "t", Group: "g"}, ErrEndpointsRequired},
		{"nil endpoints", Config{Endpoints: nil, Topic: "t", Group: "g"}, ErrEndpointsRequired},
		{"empty topic", Config{Endpoints: []string{"localhost:9876"}, Group: "g"}, ErrTopicRequired},
		{"empty group", Config{Endpoints: []string{"localhost:9876"}, Topic: "t"}, ErrGroupRequired},
		{"invalid consume model", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeModel: "weird"}, ErrInvalidConsumeModel},
		{"invalid consume from where", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: "weird"}, ErrInvalidConsumeFromWhere},
		{"timestamp missing when required", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: ConsumeFromTimestamp}, ErrInvalidConsumeTimestamp},
		{"timestamp malformed", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeFromWhere: ConsumeFromTimestamp, ConsumeTimestamp: "not-a-time"}, ErrInvalidConsumeTimestamp},
		{"negative batch max size", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", ConsumeMessageBatchMaxSize: -1}, ErrNegativeBatchMaxSize},
		{"negative cached per queue", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", MaxCachedMessagesPerQueue: -1}, ErrNegativeCachedPerQueue},
		{"negative cached per topic", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", MaxCachedMessagesPerTopic: -1}, ErrNegativeCachedPerTopic},
		{"negative retry backoff", Config{Endpoints: valid.Endpoints, Topic: "t", Group: "g", RetryBackoff: -time.Second}, ErrNegativeRetryBackoff},
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

	err := Config{ConsumeModel: "weird", ConsumeFromWhere: "weird"}.SetDefaults().Validate()
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
	if !errors.Is(err, ErrInvalidConsumeModel) {
		t.Fatalf("Validate() = %v, want consume model error", err)
	}
	if !errors.Is(err, ErrInvalidConsumeFromWhere) {
		t.Fatalf("Validate() = %v, want consume from where error", err)
	}
}

func TestConfig_MessageModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		model string
		want  rconsumer.MessageModel
	}{
		{ConsumeModelClustering, rconsumer.Clustering},
		{ConsumeModelBroadcasting, rconsumer.BroadCasting},
		{"", rconsumer.Clustering},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			t.Parallel()
			if got := (Config{ConsumeModel: tt.model}).messageModel(); got != tt.want {
				t.Errorf("messageModel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_ConsumeFromWhere(t *testing.T) {
	t.Parallel()

	tests := []struct {
		where string
		want  rconsumer.ConsumeFromWhere
	}{
		{ConsumeFromLast, rconsumer.ConsumeFromLastOffset},
		{ConsumeFromFirst, rconsumer.ConsumeFromFirstOffset},
		{ConsumeFromTimestamp, rconsumer.ConsumeFromTimestamp},
		{"", rconsumer.ConsumeFromLastOffset},
	}
	for _, tt := range tests {
		t.Run(tt.where, func(t *testing.T) {
			t.Parallel()
			if got := (Config{ConsumeFromWhere: tt.where}).consumeFromWhere(); got != tt.want {
				t.Errorf("consumeFromWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_ConsumeTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "timestamp formatted", cfg: Config{ConsumeFromWhere: ConsumeFromTimestamp, ConsumeTimestamp: "2026-01-02T03:04:05Z"}, want: "20260102030405"},
		{name: "not timestamp mode", cfg: Config{ConsumeFromWhere: ConsumeFromLast, ConsumeTimestamp: "2026-01-02T03:04:05Z"}, want: ""},
		{name: "malformed timestamp", cfg: Config{ConsumeFromWhere: ConsumeFromTimestamp, ConsumeTimestamp: "nope"}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.cfg.consumeTimestamp(); got != tt.want {
				t.Errorf("consumeTimestamp() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfig_BuildConsumerOptions(t *testing.T) {
	t.Parallel()

	base := Config{
		Endpoints: []string{"localhost:9876"},
		Topic:     "test-topic",
		Group:     "test-group",
	}.SetDefaults()

	baseCount := len(base.buildConsumerOptions("g"))
	if baseCount != 8 {
		t.Fatalf("base options = %d, want 8", baseCount)
	}

	withTimestamp := base
	withTimestamp.ConsumeFromWhere = ConsumeFromTimestamp
	withTimestamp.ConsumeTimestamp = "2026-01-01T00:00:00Z"
	if got := len(withTimestamp.buildConsumerOptions("g")); got != 9 {
		t.Fatalf("timestamp options = %d, want 9", got)
	}

	full := base
	full.ConsumeFromWhere = ConsumeFromTimestamp
	full.ConsumeTimestamp = "2026-01-01T00:00:00Z"
	full.ConsumeMessageBatchMaxSize = 32
	full.MaxCachedMessagesPerQueue = 1000
	full.MaxCachedMessagesPerTopic = 10000
	full.RetryBackoff = 3 * time.Second
	if got := len(full.buildConsumerOptions("g")); got != 13 {
		t.Fatalf("full options = %d, want 13", got)
	}
}

func TestConfigBuildMessageSelectorConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags []string
		want string
	}{
		{name: "single tag", tags: []string{"tag1"}, want: "tag1"},
		{name: "multiple tags", tags: []string{"tag1", "tag2"}, want: "tag1 || tag2"},
		{name: "wildcard", tags: []string{"*"}, want: "*"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Tags: tt.tags}
			selector := cfg.buildMessageSelectorConfig()
			if selector.Expression != tt.want {
				t.Errorf("buildMessageSelectorConfig() Expression = %q, want %q", selector.Expression, tt.want)
			}
		})
	}
}
