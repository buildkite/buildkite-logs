package logparser

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestAllHandlesLinesOverOneMiB(t *testing.T) {
	parser := New()
	content := strings.Repeat("a", 1024*1024+128)
	input := "\x1b_bk;t=1745322209921\x07" + content

	var entries []*Entry
	for entry, err := range parser.All(strings.NewReader(input)) {
		if err != nil {
			t.Fatalf("All() error = %v", err)
		}
		entries = append(entries, entry)
	}

	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Content != content {
		t.Fatalf("content length = %d, want %d", len(entries[0].Content), len(content))
	}
	if !entries[0].HasTimestamp() {
		t.Fatal("entry should have timestamp")
	}
}

func TestLineReaderHardErrorsOnLongLine(t *testing.T) {
	reader := NewLineReader(
		strings.NewReader("0123456789abcdef\nnext"),
		WithMaxLineBytes(8),
		WithContextBytes(4),
	)

	_, err := reader.Next()
	if err == nil {
		t.Fatal("expected long-line error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error type = %T, want *ParseError", err)
	}
	if parseErr.Kind != ErrorKindLineTooLong {
		t.Fatalf("kind = %s, want %s", parseErr.Kind, ErrorKindLineTooLong)
	}
	if parseErr.Line != 1 {
		t.Fatalf("line = %d, want 1", parseErr.Line)
	}
	if parseErr.LineOffset != 8 {
		t.Fatalf("line offset = %d, want 8", parseErr.LineOffset)
	}
	if len(parseErr.Before) > 4 || len(parseErr.After) > 4 {
		t.Fatalf("context exceeded limit: before=%d after=%d", len(parseErr.Before), len(parseErr.After))
	}
}

func TestLineReaderTruncatesLongLine(t *testing.T) {
	reader := NewLineReader(
		strings.NewReader("0123456789abcdef\n"),
		WithMaxLineBytes(12),
		WithTruncateLongLines(true),
		WithTruncationSuffix("[cut]"),
	)

	line, err := reader.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if !line.Truncated {
		t.Fatal("line should be marked truncated")
	}
	if got := string(line.Bytes); got != "0123456[cut]" {
		t.Fatalf("line = %q, want %q", got, "0123456[cut]")
	}
	if len(line.Bytes) > 12 {
		t.Fatalf("line length = %d, want <= 12", len(line.Bytes))
	}
}

func TestNewWithFunctionalOptions(t *testing.T) {
	parser := New(
		WithMaxLineBytes(12),
		WithTruncateLongLines(true),
		WithTruncationSuffix("[cut]"),
	)

	var entries []*Entry
	for entry, err := range parser.All(strings.NewReader("0123456789abcdef\n")) {
		if err != nil {
			t.Fatalf("All() error = %v", err)
		}
		entries = append(entries, entry)
	}

	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if got := entries[0].Content; got != "0123456[cut]" {
		t.Fatalf("content = %q, want %q", got, "0123456[cut]")
	}
}

func TestParserUnterminatedOSCTreatedAsPlainContent(t *testing.T) {
	parser := New(WithContextBytes(3))
	input := "\x1b_bk;t=123456"
	entry, err := parser.ParseLine(input)
	if err != nil {
		t.Fatalf("ParseLine() error = %v", err)
	}
	if entry.Content != input {
		t.Fatalf("content = %q, want %q", entry.Content, input)
	}
	if entry.HasTimestamp() {
		t.Fatal("unterminated OSC entry should not have timestamp")
	}
	if string(entry.RawLine) != input {
		t.Fatalf("raw line = %q, want %q", string(entry.RawLine), input)
	}
}

func TestParseErrorStringOmitsContextBytes(t *testing.T) {
	reader := NewLineReader(
		strings.NewReader("prefix_SECRET_TOKEN_123_suffix\n"),
		WithMaxLineBytes(8),
		WithContextBytes(32),
	)
	_, err := reader.Next()
	if err == nil {
		t.Fatal("expected line-too-long error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error type = %T, want *ParseError", err)
	}

	errText := err.Error()
	if strings.Contains(errText, "SECRET_TOKEN_123") {
		t.Fatalf("Error() leaked context bytes: %q", errText)
	}
	if !strings.Contains(errText, "line 1") {
		t.Fatalf("Error() missing line: %q", errText)
	}
	if !strings.Contains(errText, "stream offset "+strconv.FormatInt(parseErr.StreamOffset, 10)) {
		t.Fatalf("Error() missing stream offset: %q", errText)
	}
	if !strings.Contains(errText, "line offset "+strconv.Itoa(parseErr.LineOffset)) {
		t.Fatalf("Error() missing line offset: %q", errText)
	}
}

func TestParseErrorAllowsZeroContextBytes(t *testing.T) {
	reader := NewLineReader(
		strings.NewReader("prefix_SECRET_TOKEN_123_suffix\n"),
		WithMaxLineBytes(8),
		WithContextBytes(0),
	)
	_, err := reader.Next()
	if err == nil {
		t.Fatal("expected line-too-long error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error type = %T, want *ParseError", err)
	}
	if len(parseErr.Before) != 0 || len(parseErr.After) != 0 {
		t.Fatalf("context lengths = before %d after %d, want both zero", len(parseErr.Before), len(parseErr.After))
	}
}

func TestLineReaderNoFinalNewline(t *testing.T) {
	reader := NewLineReader(strings.NewReader("first\nsecond"))

	line, err := reader.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if string(line.Bytes) != "first" {
		t.Fatalf("first line = %q", string(line.Bytes))
	}

	line, err = reader.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if string(line.Bytes) != "second" {
		t.Fatalf("second line = %q", string(line.Bytes))
	}

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("final error = %v, want EOF", err)
	}
}
