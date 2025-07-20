package buildkitelogs

import (
	"testing"
	"time"
)

func TestByteParserBasic(t *testing.T) {
	parser := NewByteParser()

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
			name:        "Invalid OSC sequence",
			input:       "\x1b_bk;t=invalid\x07content",
			wantTs:      0,
			wantContent: "\x1b_bk;t=invalid\x07content",
			wantHasTs:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parser.ParseLine(tt.input)

			if tt.name == "Invalid OSC sequence" {
				if err == nil {
					t.Error("Expected error for invalid timestamp")
				}
				return
			}

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

func TestByteParserEdgeCases(t *testing.T) {
	parser := NewByteParser()

	tests := []struct {
		name  string
		input string
	}{
		{"Empty string", ""},
		{"Very short string", "a"},
		{"OSC start without BEL", "\x1b_bk;t=123456"},
		{"OSC with multiple BEL", "\x1b_bk;t=123\x07content\x07more"},
		{"Large timestamp", "\x1b_bk;t=9999999999999\x07content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			entry, err := parser.ParseLine(tt.input)

			// For invalid cases, either return original line or error
			if err != nil {
				// Error is acceptable for malformed input
				return
			}

			if entry == nil {
				t.Error("ParseLine() returned nil entry")
			}
		})
	}
}
