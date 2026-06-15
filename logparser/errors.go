package logparser

import (
	"fmt"
)

type ErrorKind string

const (
	ErrorKindReadFailure ErrorKind = "read_failure"
	ErrorKindLineTooLong ErrorKind = "line_too_long"
)

// ParseError describes where and why parsing failed, with a small byte window
// around the failure to make malformed log lines easier to inspect.
type ParseError struct {
	Kind         ErrorKind
	Line         int
	StreamOffset int64
	LineOffset   int
	Before       []byte
	After        []byte
	Err          error
}

func (e *ParseError) Error() string {
	if e == nil {
		return "<nil>"
	}

	msg := string(e.Kind)
	if e.Err != nil {
		msg = e.Err.Error()
	}

	return fmt.Sprintf("parse error at line %d, stream offset %d, line offset %d: %s",
		e.Line, e.StreamOffset, e.LineOffset, msg)
}

func (e *ParseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newParseError(kind ErrorKind, line int, streamOffset int64, lineOffset int, data []byte, contextBytes int, err error) *ParseError {
	if lineOffset < 0 {
		lineOffset = 0
	}
	if lineOffset > len(data) {
		lineOffset = len(data)
	}

	beforeStart := lineOffset - contextBytes
	if beforeStart < 0 {
		beforeStart = 0
	}
	afterEnd := lineOffset + contextBytes
	if afterEnd > len(data) {
		afterEnd = len(data)
	}

	before := append([]byte(nil), data[beforeStart:lineOffset]...)
	after := append([]byte(nil), data[lineOffset:afterEnd]...)

	return &ParseError{
		Kind:         kind,
		Line:         line,
		StreamOffset: streamOffset + int64(lineOffset),
		LineOffset:   lineOffset,
		Before:       before,
		After:        after,
		Err:          err,
	}
}
