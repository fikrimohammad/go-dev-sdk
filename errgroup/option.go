package errgroup

var (
	DefaultMaxConcurrency = 20
)

type options struct {
	maxConcurrency int
}

type Option func(*options)

func WithMaxConcurrency(concurrency int) Option {
	return func(opts *options) {
		opts.maxConcurrency = concurrency
	}
}
