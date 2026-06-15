package logparser

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func FuzzParseLine(f *testing.F) {
	seeds := []string{
		"",
		"plain log line",
		"\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
		"\x1b_bk;t=invalid\x07content",
		"\x1b_bk;t=1745322209921",
		"\x1b_bk;t=1745322209921\x07first\x1b_bk;t=1745322209922\x07second",
		"\x1b_bk;t=1745322209921\x07\x1b[38;5;48mcolored\x1b[0m",
		strings.Repeat("a", 4097),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1<<20 {
			t.Skip("keep fuzz cases bounded")
		}

		parser := New(Options{
			MaxLineBytes: 4096,
			ContextBytes: 32,
		})
		entry, err := parser.ParseLine(input)
		if err != nil {
			assertParseErrorBounds(t, err, len(input), 32)
			return
		}
		if entry == nil {
			t.Fatal("entry is nil without error")
		}
		if len(entry.RawLine) != len(input) {
			t.Fatalf("raw line length = %d, want %d", len(entry.RawLine), len(input))
		}
	})
}

func FuzzLineReader(f *testing.F) {
	seeds := []string{
		"",
		"one line",
		"first\nsecond\nthird",
		"\x1b_bk;t=1745322209921\x07content\nunterminated",
		strings.Repeat("x", 8192),
		strings.Repeat("x", 8192) + "\nnext",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1<<20 {
			t.Skip("keep fuzz cases bounded")
		}

		reader := NewLineReader(strings.NewReader(input), Options{
			MaxLineBytes:      4096,
			TruncateLongLines: true,
			TruncationSuffix:  "[truncated]",
			ContextBytes:      32,
		})

		for {
			line, err := reader.Next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				assertParseErrorBounds(t, err, len(input), 32)
				return
			}
			if len(line.Bytes) > 4096 {
				t.Fatalf("line length = %d, want <= 4096", len(line.Bytes))
			}
			if line.Number <= 0 {
				t.Fatalf("line number = %d, want positive", line.Number)
			}
			if line.StreamOffset < 0 {
				t.Fatalf("stream offset = %d, want non-negative", line.StreamOffset)
			}
		}
	})
}

func FuzzParserAll(f *testing.F) {
	seeds := []string{
		"",
		"plain\nlines",
		"\x1b_bk;t=1745322209921\x07~~~ group\n\x1b_bk;t=1745322209922\x07content",
		"\x1b_bk;t=invalid\x07~~~ group\ncontent",
		"\x1b_bk;t=1745322209921",
		strings.Repeat("z", 8192) + "\nnext",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1<<20 {
			t.Skip("keep fuzz cases bounded")
		}

		parser := New(Options{
			MaxLineBytes:      4096,
			TruncateLongLines: true,
			ContextBytes:      32,
		})
		for entry, err := range parser.All(strings.NewReader(input)) {
			if err != nil {
				assertParseErrorBounds(t, err, len(input), 32)
				return
			}
			if entry == nil {
				t.Fatal("entry is nil without error")
			}
			if len(entry.RawLine) > 4096 {
				t.Fatalf("raw line length = %d, want <= 4096", len(entry.RawLine))
			}
			if entry.Group != "" && !entry.IsGroup() && len(entry.Group) > 4096 {
				t.Fatalf("group length = %d, want <= 4096", len(entry.Group))
			}
		}
	})
}

func assertParseErrorBounds(t *testing.T, err error, inputLen int, contextBytes int) {
	t.Helper()

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		return
	}
	if parseErr.Line <= 0 {
		t.Fatalf("parse error line = %d, want positive", parseErr.Line)
	}
	if parseErr.LineOffset < 0 {
		t.Fatalf("parse error line offset = %d, want non-negative", parseErr.LineOffset)
	}
	if parseErr.StreamOffset < 0 || parseErr.StreamOffset > int64(inputLen)+1 {
		t.Fatalf("parse error stream offset = %d outside input length %d", parseErr.StreamOffset, inputLen)
	}
	if len(parseErr.Before) > contextBytes {
		t.Fatalf("before context = %d, want <= %d", len(parseErr.Before), contextBytes)
	}
	if len(parseErr.After) > contextBytes {
		t.Fatalf("after context = %d, want <= %d", len(parseErr.After), contextBytes)
	}
}
