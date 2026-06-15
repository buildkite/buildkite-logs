package buildkitelogs

import (
	"strings"
	"testing"
	"time"

	"github.com/buildkite/buildkite-logs/logparser"
)

// TestParseLine exercises Parser.ParseLine specifically (the wrapper that adds
// group tracking on top of ByteParser.ParseLine). For byte-level parsing tests,
// see TestByteParserBasic in scanner_test.go.
func TestParseLine(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	tests := []struct {
		name        string
		input       string
		wantTs      int64
		wantContent string
		wantHasTs   bool
	}{
		{
			name:        "OSC sequence with timestamp",
			input:       "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
			wantTs:      1745322209921,
			wantContent: "~~~ Running global environment hook",
			wantHasTs:   true,
		},
		{
			name:        "OSC sequence with ANSI codes",
			input:       "\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment",
			wantTs:      1745322209921,
			wantContent: "[90m$[0m /buildkite/agent/hooks/environment",
			wantHasTs:   true,
		},
		{
			name:        "Regular line without OSC",
			input:       "regular log line",
			wantTs:      0,
			wantContent: "regular log line",
			wantHasTs:   false,
		},
		{
			name:        "Empty OSC content",
			input:       "\x1b_bk;t=1745322209921\x07",
			wantTs:      1745322209921,
			wantContent: "",
			wantHasTs:   true,
		},
		{
			name:        "Invalid OSC timestamp",
			input:       "\x1b_bk;t=invalid\x07content",
			wantTs:      0,
			wantContent: "content",
			wantHasTs:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parser.ParseLine(tt.input)
			if err != nil {
				t.Fatalf("ParseLine() error = %v", err)
			}

			if entry.Content != tt.wantContent {
				t.Errorf("ParseLine() content = %q, want %q", entry.Content, tt.wantContent)
			}

			if tt.wantHasTs {
				expectedTime := time.Unix(0, tt.wantTs*int64(time.Millisecond))
				if !entry.Timestamp.Equal(expectedTime) {
					t.Errorf("ParseLine() timestamp = %v, want %v", entry.Timestamp, expectedTime)
				}
			}

			if entry.HasTimestamp() != tt.wantHasTs {
				t.Errorf("ParseLine() HasTimestamp() = %v, want %v", entry.HasTimestamp(), tt.wantHasTs)
			}

			if string(entry.RawLine) != tt.input {
				t.Errorf("ParseLine() RawLine = %q, want %q", string(entry.RawLine), tt.input)
			}
		})
	}
}

func TestLogEntryClassification(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	tests := []struct {
		name      string
		input     string
		wantGroup bool
	}{
		{
			name:      "Command with ANSI",
			input:     "\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment",
			wantGroup: false,
		},
		{
			name:      "Section header",
			input:     "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
			wantGroup: true,
		},
		{
			name:      "Build artifact section",
			input:     "\x1b_bk;t=1745322210701\x07+++ :frame_with_picture: Inline image uploaded",
			wantGroup: true,
		},
		{
			name:      "Regular output",
			input:     "\x1b_bk;t=1745322210701\x07Cloning into '.'...",
			wantGroup: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parser.ParseLine(tt.input)
			if err != nil {
				t.Fatalf("ParseLine() error = %v", err)
			}

			if entry.IsGroup() != tt.wantGroup {
				t.Errorf("IsGroup() = %v, want %v", entry.IsGroup(), tt.wantGroup)
			}

		})
	}
}

func TestParseReader(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	input := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment\n" +
		"regular log line without OSC\n" +
		"\x1b_bk;t=1745322209948\x07~~~ Running global pre-checkout hook"

	reader := strings.NewReader(input)

	// Collect entries using streaming iterator
	var entries []*logparser.Entry
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("Parser.All() error = %v", err)
		}
		entries = append(entries, entry)
	}

	if len(entries) != 4 {
		t.Fatalf("Parser.All() got %d entries, want 4", len(entries))
	}

	// Check first entry
	if !entries[0].HasTimestamp() {
		t.Error("First entry should have timestamp")
	}
	if !entries[0].IsGroup() {
		t.Error("First entry should be a group")
	}

	// Check second entry
	if !entries[1].HasTimestamp() {
		t.Error("Second entry should have timestamp")
	}

	// Check third entry (regular line)
	if entries[2].HasTimestamp() {
		t.Error("Third entry should not have timestamp")
	}
	if entries[2].Content != "regular log line without OSC" {
		t.Errorf("Third entry content = %q, want %q", entries[2].Content, "regular log line without OSC")
	}

	// Check fourth entry
	if !entries[3].HasTimestamp() {
		t.Error("Fourth entry should have timestamp")
	}
	if !entries[3].IsGroup() {
		t.Error("Fourth entry should be a group")
	}
}

func TestParserAllCompatibility(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	input := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment\n" +
		"regular log line without OSC\n" +
		"\x1b_bk;t=1745322209948\x07~~~ Running global pre-checkout hook"

	reader := strings.NewReader(input)
	var entries []*logparser.Entry
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("All() error: %v", err)
		}
		entries = append(entries, entry)
	}

	if len(entries) != 4 {
		t.Fatalf("Expected 4 entries, got %d", len(entries))
	}

	// Test first entry
	entry := entries[0]
	if !entry.HasTimestamp() {
		t.Error("First entry should have timestamp")
	}
	if !entry.IsGroup() {
		t.Error("First entry should be a group")
	}

	// Test second entry
	entry = entries[1]
	if !entry.HasTimestamp() {
		t.Error("Second entry should have timestamp")
	}

	// Test third entry (regular line)
	entry = entries[2]
	if entry.HasTimestamp() {
		t.Error("Third entry should not have timestamp")
	}
	if entry.Content != "regular log line without OSC" {
		t.Errorf("Third entry content = %q, want %q", entry.Content, "regular log line without OSC")
	}

	// Test fourth entry
	entry = entries[3]
	if !entry.HasTimestamp() {
		t.Error("Fourth entry should have timestamp")
	}
	if !entry.IsGroup() {
		t.Error("Fourth entry should be a group")
	}

}

func TestLogIteratorEmpty(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())
	reader := strings.NewReader("")

	for range parser.All(reader) {
		t.Error("Should not have entries for empty input")
	}
}

func TestLogIteratorInvalidTimestamp(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	// An overflowing timestamp in valid OSC framing should not cause an error;
	// the line is returned with a zero timestamp and the OSC envelope stripped.
	input := "\x1b_bk;t=999999999999999999999999999999\x07content"
	reader := strings.NewReader(input)
	var entries []*logparser.Entry
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("All() error: %v", err)
		}
		entries = append(entries, entry)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected one entry for invalid timestamp, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Content != "content" {
		t.Errorf("Expected content %q, got %q", "content", entry.Content)
	}
	if entry.HasTimestamp() {
		t.Error("Expected HasTimestamp() to be false")
	}

}

func TestComputeFlags(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantTimestamp bool
		wantGroup     bool
	}{
		{
			name:          "timestamped group header",
			input:         "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
			wantTimestamp: true,
			wantGroup:     true,
		},
		{
			name:          "timestamped regular line",
			input:         "\x1b_bk;t=1745322209921\x07some output",
			wantTimestamp: true,
			wantGroup:     false,
		},
		{
			name:          "plain group header",
			input:         "~~~ Running global environment hook",
			wantTimestamp: false,
			wantGroup:     true,
		},
		{
			name:          "plain regular line",
			input:         "some output",
			wantTimestamp: false,
			wantGroup:     false,
		},
	}

	parser := logparser.New(logparser.DefaultOptions())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parser.ParseLine(tt.input)
			if err != nil {
				t.Fatalf("ParseLine() error = %v", err)
			}

			flags := entry.ComputeFlags()

			if flags.Has(logparser.HasTimestamp) != tt.wantTimestamp {
				t.Errorf("HasTimestamp flag = %v, want %v", flags.Has(logparser.HasTimestamp), tt.wantTimestamp)
			}
			if flags.Has(logparser.IsGroup) != tt.wantGroup {
				t.Errorf("IsGroup flag = %v, want %v", flags.Has(logparser.IsGroup), tt.wantGroup)
			}
		})
	}
}

func TestParseLineEdgeCases(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	inputs := []string{
		"",
		"a",
		"\x1b_bk;t=123\x07content\x07more",   // OSC with multiple BEL
		"\x1b_bk;t=9999999999999\x07content", // Large timestamp
	}

	for _, input := range inputs {
		entry, err := parser.ParseLine(input)
		if err != nil {
			t.Errorf("ParseLine(%q) returned unexpected error: %v", input, err)
			continue
		}
		if entry == nil {
			t.Errorf("ParseLine(%q) returned nil entry", input)
		}
	}
}

func TestParseLineMultiOSCTruncation(t *testing.T) {
	parser := logparser.New(logparser.DefaultOptions())

	input := "\x1b_bk;t=1745322209921\x07first content\x1b_bk;t=1745322209922\x07second content"
	entry, err := parser.ParseLine(input)
	if err != nil {
		t.Fatalf("ParseLine() error = %v", err)
	}

	if entry.Content != "first content" {
		t.Errorf("Content = %q, want %q", entry.Content, "first content")
	}

	if string(entry.RawLine) != input {
		t.Errorf("RawLine = %q, want %q", string(entry.RawLine), input)
	}
}
