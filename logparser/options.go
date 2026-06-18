package logparser

const (
	DefaultBufferSize       = 64 * 1024
	DefaultMaxLineBytes     = 8 * 1024 * 1024
	DefaultContextBytes     = 64
	DefaultTruncationSuffix = "... [truncated]"
)

// Options configures parser and line-reader behavior.
type Options struct {
	BufferSize        int
	MaxLineBytes      int
	TruncateLongLines bool
	TruncationSuffix  string
	ContextBytes      int
}

// Option customizes parser behavior.
type Option interface {
	apply(*Options)
}

type optionFunc func(*Options)

func (f optionFunc) apply(opts *Options) {
	f(opts)
}

// DefaultOptions returns conservative defaults for preserving log data while
// supporting lines that exceed bufio.Scanner's token limit.
func DefaultOptions() Options {
	return Options{
		BufferSize:       DefaultBufferSize,
		MaxLineBytes:     DefaultMaxLineBytes,
		TruncationSuffix: DefaultTruncationSuffix,
		ContextBytes:     DefaultContextBytes,
	}
}

// WithBufferSize sets the buffered reader size used while reading log lines.
func WithBufferSize(size int) Option {
	return optionFunc(func(opts *Options) {
		opts.BufferSize = size
	})
}

// WithMaxLineBytes sets the maximum bytes allowed for a single log line.
func WithMaxLineBytes(size int) Option {
	return optionFunc(func(opts *Options) {
		opts.MaxLineBytes = size
	})
}

// WithTruncateLongLines controls whether over-limit log lines are truncated
// instead of returned as line-too-long parse errors.
func WithTruncateLongLines(truncate bool) Option {
	return optionFunc(func(opts *Options) {
		opts.TruncateLongLines = truncate
	})
}

// WithTruncationSuffix sets the suffix appended to truncated log lines.
func WithTruncationSuffix(suffix string) Option {
	return optionFunc(func(opts *Options) {
		opts.TruncationSuffix = suffix
	})
}

// WithContextBytes sets how many nearby bytes are captured in parse errors.
func WithContextBytes(size int) Option {
	return optionFunc(func(opts *Options) {
		opts.ContextBytes = size
	})
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaults.BufferSize
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = defaults.MaxLineBytes
	}
	if opts.TruncationSuffix == "" {
		opts.TruncationSuffix = defaults.TruncationSuffix
	}
	if opts.ContextBytes < 0 {
		opts.ContextBytes = 0
	}
	return opts
}
