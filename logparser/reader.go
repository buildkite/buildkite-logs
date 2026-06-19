package logparser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// Line contains a single logical log line with stream position metadata.
type Line struct {
	Number       int
	StreamOffset int64
	Bytes        []byte
	Truncated    bool
}

// LineReader reads newline-delimited log records without bufio.Scanner's token
// limit, while still applying a caller-controlled maximum line size.
type LineReader struct {
	reader *bufio.Reader
	opts   Options
	line   int
	offset int64
}

func NewLineReader(r io.Reader, options ...Option) *LineReader {
	return newLineReaderWithOptions(r, optionsFrom(options...))
}

func newLineReaderWithOptions(r io.Reader, opts Options) *LineReader {
	return &LineReader{
		reader: bufio.NewReaderSize(r, opts.BufferSize),
		opts:   opts,
	}
}

func (lr *LineReader) Next() (Line, error) {
	lineNumber := lr.line + 1
	lineOffset := lr.offset
	var buf []byte
	consumed := 0
	truncated := false

readLine:
	for {
		fragment, err := lr.reader.ReadSlice('\n')
		if len(fragment) > 0 {
			consumed += len(fragment)

			if len(buf)+len(fragment) > lr.opts.MaxLineBytes {
				if !lr.opts.TruncateLongLines {
					buf = appendUpTo(buf, fragment, lr.opts.MaxLineBytes)
					consumed += lr.discardLineRemainder(err)
					lr.offset += int64(consumed)
					lr.line++
					return Line{}, newParseError(
						ErrorKindLineTooLong,
						lineNumber,
						lineOffset,
						lr.opts.MaxLineBytes,
						buf,
						lr.opts.ContextBytes,
						errors.New("line exceeds configured maximum length"),
					)
				}

				buf = appendUpTo(buf, fragment, lr.opts.MaxLineBytes)
				consumed += lr.discardLineRemainder(err)
				truncated = true
				break
			}

			buf = append(buf, fragment...)
		}

		switch {
		case err == nil:
			break readLine
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(buf) == 0 {
				return Line{}, io.EOF
			}
			break readLine
		default:
			lr.offset += int64(consumed)
			return Line{}, newParseError(
				ErrorKindReadFailure,
				lineNumber,
				lineOffset,
				len(buf),
				buf,
				lr.opts.ContextBytes,
				err,
			)
		}
	}

	lineBytes := trimLineEnding(buf)
	if truncated {
		lineBytes = applyTruncationSuffix(lineBytes, lr.opts.MaxLineBytes, []byte(lr.opts.TruncationSuffix))
	}

	lr.offset += int64(consumed)
	lr.line++

	return Line{
		Number:       lineNumber,
		StreamOffset: lineOffset,
		Bytes:        lineBytes,
		Truncated:    truncated,
	}, nil
}

func (lr *LineReader) discardLineRemainder(currentErr error) int {
	if currentErr == nil || errors.Is(currentErr, io.EOF) {
		return 0
	}

	discarded := 0
	for {
		fragment, err := lr.reader.ReadSlice('\n')
		discarded += len(fragment)
		if err == nil || errors.Is(err, io.EOF) {
			return discarded
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return discarded
		}
	}
}

func appendUpTo(dst, src []byte, max int) []byte {
	remaining := max - len(dst)
	if remaining <= 0 {
		return dst
	}
	if len(src) > remaining {
		src = src[:remaining]
	}
	return append(dst, src...)
}

func trimLineEnding(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\n'})
	line = bytes.TrimSuffix(line, []byte{'\r'})
	return line
}

func applyTruncationSuffix(line []byte, max int, suffix []byte) []byte {
	if max <= 0 {
		return nil
	}
	if len(suffix) >= max {
		return append([]byte(nil), suffix[:max]...)
	}

	out := make([]byte, 0, max)
	prefixLen := max - len(suffix)
	if prefixLen > len(line) {
		prefixLen = len(line)
	}
	out = append(out, line[:prefixLen]...)
	out = append(out, suffix...)
	return out
}
