package buildkitelogs

import (
	"strings"
	"testing"
	"time"
)

func TestParseLine(t *testing.T) {
	parser := NewParser()

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

func TestStripANSI(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "ANSI color codes",
			input: "[90m$[0m /buildkite/agent/hooks/environment",
			want:  "$ /buildkite/agent/hooks/environment",
		},
		{
			name:  "No ANSI codes",
			input: "plain text",
			want:  "plain text",
		},
		{
			name:  "Complex ANSI sequence",
			input: "[38;5;48m2025-04-22 11:43:30 INFO[0m [0mFound 2 files[0m",
			want:  "2025-04-22 11:43:30 INFO Found 2 files",
		},
		{
			name:  "ANSI with K sequence",
			input: "remote: Counting objects: 100% (54/54)[K",
			want:  "remote: Counting objects: 100% (54/54)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parser.StripANSI(tt.input)
			if got != tt.want {
				t.Errorf("StripANSI() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLogEntryClassification(t *testing.T) {
	parser := NewParser()

	tests := []struct {
		name        string
		input       string
		wantCommand bool
		wantSection bool
	}{
		{
			name:        "Command with ANSI",
			input:       "\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment",
			wantCommand: true,
			wantSection: false,
		},
		{
			name:        "Section header",
			input:       "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
			wantCommand: false,
			wantSection: true,
		},
		{
			name:        "Build artifact section",
			input:       "\x1b_bk;t=1745322210701\x07+++ :frame_with_picture: Inline image uploaded",
			wantCommand: false,
			wantSection: true,
		},
		{
			name:        "Regular output",
			input:       "\x1b_bk;t=1745322210701\x07Cloning into '.'...",
			wantCommand: false,
			wantSection: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parser.ParseLine(tt.input)
			if err != nil {
				t.Fatalf("ParseLine() error = %v", err)
			}

			if entry.IsCommand() != tt.wantCommand {
				t.Errorf("IsCommand() = %v, want %v", entry.IsCommand(), tt.wantCommand)
			}

			if entry.IsSection() != tt.wantSection {
				t.Errorf("IsSection() = %v, want %v", entry.IsSection(), tt.wantSection)
			}

		})
	}
}

func TestParseReader(t *testing.T) {
	parser := NewParser()

	input := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment\n" +
		"regular log line without OSC\n" +
		"\x1b_bk;t=1745322209948\x07~~~ Running global pre-checkout hook"

	reader := strings.NewReader(input)

	// Collect entries using streaming iterator
	var entries []*LogEntry
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
	if !entries[0].IsSection() {
		t.Error("First entry should be a section")
	}

	// Check second entry
	if !entries[1].HasTimestamp() {
		t.Error("Second entry should have timestamp")
	}
	if !entries[1].IsCommand() {
		t.Error("Second entry should be a command")
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
	if !entries[3].IsSection() {
		t.Error("Fourth entry should be a section")
	}
}

func TestLogIterator(t *testing.T) {
	parser := NewParser()

	input := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment\n" +
		"regular log line without OSC\n" +
		"\x1b_bk;t=1745322209948\x07~~~ Running global pre-checkout hook"

	reader := strings.NewReader(input)
	iterator := parser.NewIterator(reader)

	// Test first entry
	if !iterator.Next() {
		t.Fatal("Expected first entry")
	}
	entry := iterator.Entry()
	if !entry.HasTimestamp() {
		t.Error("First entry should have timestamp")
	}
	if !entry.IsSection() {
		t.Error("First entry should be a section")
	}

	// Test second entry
	if !iterator.Next() {
		t.Fatal("Expected second entry")
	}
	entry = iterator.Entry()
	if !entry.HasTimestamp() {
		t.Error("Second entry should have timestamp")
	}
	if !entry.IsCommand() {
		t.Error("Second entry should be a command")
	}

	// Test third entry (regular line)
	if !iterator.Next() {
		t.Fatal("Expected third entry")
	}
	entry = iterator.Entry()
	if entry.HasTimestamp() {
		t.Error("Third entry should not have timestamp")
	}
	if entry.Content != "regular log line without OSC" {
		t.Errorf("Third entry content = %q, want %q", entry.Content, "regular log line without OSC")
	}

	// Test fourth entry
	if !iterator.Next() {
		t.Fatal("Expected fourth entry")
	}
	entry = iterator.Entry()
	if !entry.HasTimestamp() {
		t.Error("Fourth entry should have timestamp")
	}
	if !entry.IsSection() {
		t.Error("Fourth entry should be a section")
	}

	// Test end of input
	if iterator.Next() {
		t.Error("Should not have more entries")
	}

	// Check no errors occurred
	if iterator.Err() != nil {
		t.Errorf("Iterator error: %v", iterator.Err())
	}
}

func TestLogIteratorEmpty(t *testing.T) {
	parser := NewParser()
	reader := strings.NewReader("")
	iterator := parser.NewIterator(reader)

	if iterator.Next() {
		t.Error("Should not have entries for empty input")
	}

	if iterator.Err() != nil {
		t.Errorf("Iterator error on empty input: %v", iterator.Err())
	}
}

func TestLogIteratorError(t *testing.T) {
	parser := NewParser()

	// Create a reader that will cause an error after first read
	input := "\x1b_bk;t=999999999999999999999999999999\x07content"
	reader := strings.NewReader(input)
	iterator := parser.NewIterator(reader)

	// This should fail due to invalid timestamp
	if iterator.Next() {
		t.Error("Expected Next() to return false due to parse error")
	}

	if iterator.Err() == nil {
		t.Error("Expected error for invalid timestamp")
	}
}
