package errgroup

// DefaultMaxConcurrency is the concurrency limit New uses when WithMaxConcurrency is not given.
const DefaultMaxConcurrency = 20

type options struct {
	maxConcurrency int
}

// Option configures a Group created by New or SubGroup.
type Option func(*options)

// WithMaxConcurrency sets the maximum number of tasks running at once. A value <= 0 means no limit.
func WithMaxConcurrency(concurrency int) Option {
	return func(opts *options) {
		opts.maxConcurrency = concurrency
	}
}
