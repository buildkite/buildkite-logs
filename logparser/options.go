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
