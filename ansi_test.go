package buildkitelogs

import (
	"strings"
	"testing"
)

func TestStripANSI(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no ansi codes",
			input:    "hello world",
			expected: "hello world",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "simple color reset",
			input:    "\x1b[0mhello",
			expected: "hello",
		},
		{
			name:     "color codes",
			input:    "\x1b[31mred text\x1b[0m normal",
			expected: "red text normal",
		},
		{
			name:     "complex color codes",
			input:    "\x1b[1;31;40mbold red on black\x1b[0m",
			expected: "bold red on black",
		},
		{
			name:     "multiple sequences",
			input:    "\x1b[31mred\x1b[32m green\x1b[0m normal",
			expected: "red green normal",
		},
		{
			name:     "cursor movement",
			input:    "\x1b[2J\x1b[Hclear screen",
			expected: "clear screen",
		},
		{
			name:     "buildkite log line",
			input:    "\x1b[90m2023-01-01 12:00:00\x1b[0m \x1b[32m[INFO]\x1b[0m Starting build",
			expected: "2023-01-01 12:00:00 [INFO] Starting build",
		},
		{
			name:     "osc sequence with bell",
			input:    "\x1b]0;Window Title\x07hello",
			expected: "hello",
		},
		{
			name:     "osc sequence with escape",
			input:    "\x1b]0;Window Title\x1b\\hello",
			expected: "hello",
		},
		{
			name:     "dcs sequence",
			input:    "\x1bP+q544e\x1b\\hello",
			expected: "hello",
		},
		{
			name:     "real buildkite error log",
			input:    "\x1b[31;1mError:\x1b[0m \x1b[31mtest failed\x1b[0m\nDetails: \x1b[33mcheck logs\x1b[0m",
			expected: "Error: test failed\nDetails: check logs",
		},
		{
			name:     "mixed content",
			input:    "before \x1b[31mred\x1b[0m middle \x1b[32mgreen\x1b[0m after",
			expected: "before red middle green after",
		},
		{
			name:     "escape at end",
			input:    "hello world\x1b[0m",
			expected: "hello world",
		},
		{
			name:     "malformed escape incomplete",
			input:    "hello \x1b[31",
			expected: "hello ",
		},
		{
			name:     "just escape",
			input:    "\x1b",
			expected: "",
		},
		{
			name:     "unicode with ansi",
			input:    "\x1b[31m🔥 Error: \x1b[0mUnicode test ✅",
			expected: "🔥 Error: Unicode test ✅",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StripANSI(tt.input)
			if result != tt.expected {
				t.Errorf("StripANSI() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestStripANSILargeInput(t *testing.T) {
	// Test with a large input to ensure memory efficiency
	var builder strings.Builder
	for i := 0; i < 1000; i++ {
		builder.WriteString("\x1b[31mred line ")
		builder.WriteString(strings.Repeat("x", 100))
		builder.WriteString("\x1b[0m\n")
	}

	largeInput := builder.String()
	result := StripANSI(largeInput)

	// Verify ANSI codes were actually removed
	if strings.Contains(result, "\x1b[") {
		t.Error("ANSI codes not fully removed from large input")
	}
}

func TestStripANSIEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"consecutive escapes", "\x1b[31m\x1b[32m\x1b[0mtext"},
		{"escape without sequence", "\x1balone"},
		{"partial sequences", "\x1b[\x1b]"},
		{"null bytes", "\x1b[31m\x00text\x1b[0m"},
		{"very long sequence", "\x1b[" + strings.Repeat("1;", 100) + "mtext\x1b[0m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic or cause issues
			result := StripANSI(tt.input)
			_ = result // Use the result to ensure it doesn't get optimized away
		})
	}
}

func TestStripANSIConsistency(t *testing.T) {
	// Ensure both implementations produce the same results
	testCases := []string{
		"hello world",
		"\x1b[31mred text\x1b[0m",
		"\x1b[1;31;40mbold red on black\x1b[0m",
		"\x1b[2J\x1b[H\x1b[31mcomplex\x1b[0m",
		"\x1b]0;title\x07content",
		"\x1bP+q544e\x1b\\dcs",
		"mixed \x1b[31mred\x1b[0m and \x1b[32mgreen\x1b[0m text",
		"",
		"\x1b",
		"\x1b[",
		"\x1b[31",
		"real world \x1b[90m[2023-01-01 12:00:00]\x1b[0m \x1b[32m✓\x1b[0m test passed",
	}

	for _, input := range testCases {
		t.Run("input_"+input, func(t *testing.T) {
			regex := StripANSIRegex(input)
			custom := StripANSI(input)

			if regex != custom {
				t.Errorf("StripANSIRegex != StripANSI for input %q: %q vs %q", input, regex, custom)
			}
		})
	}
}

func TestParquetLogEntryCleanMethods(t *testing.T) {
	tests := []struct {
		name        string
		entry       ParquetLogEntry
		stripANSI   bool
		wantContent string
		wantGroup   string
	}{
		{
			name: "no ansi, no whitespace",
			entry: ParquetLogEntry{
				Content: "hello world",
				Group:   "test group",
			},
			stripANSI:   false,
			wantContent: "hello world",
			wantGroup:   "test group",
		},
		{
			name: "with ansi, strip disabled",
			entry: ParquetLogEntry{
				Content: "\x1b[31mred text\x1b[0m",
				Group:   "\x1b[32mgreen group\x1b[0m",
			},
			stripANSI:   false,
			wantContent: "\x1b[31mred text\x1b[0m",
			wantGroup:   "\x1b[32mgreen group\x1b[0m",
		},
		{
			name: "with ansi, strip enabled",
			entry: ParquetLogEntry{
				Content: "\x1b[31mred text\x1b[0m",
				Group:   "\x1b[32mgreen group\x1b[0m",
			},
			stripANSI:   true,
			wantContent: "red text",
			wantGroup:   "green group",
		},
		{
			name: "with whitespace trim",
			entry: ParquetLogEntry{
				Content: "  \t  hello world  \n  ",
				Group:   "  \n test group \t ",
			},
			stripANSI:   false,
			wantContent: "hello world",
			wantGroup:   "test group",
		},
		{
			name: "ansi and whitespace combined",
			entry: ParquetLogEntry{
				Content: "  \x1b[31m  red text  \x1b[0m  ",
				Group:   "\n  \x1b[32mgreen group\x1b[0m \t ",
			},
			stripANSI:   true,
			wantContent: "red text",
			wantGroup:   "green group",
		},
		{
			name: "complex buildkite log",
			entry: ParquetLogEntry{
				Content: " \x1b[90m2023-01-01 12:00:00\x1b[0m \x1b[32m[INFO]\x1b[0m Starting build ",
				Group:   " \x1b[1mTest Group\x1b[0m ",
			},
			stripANSI:   true,
			wantContent: "2023-01-01 12:00:00 [INFO] Starting build",
			wantGroup:   "Test Group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotContent := tt.entry.CleanContent(tt.stripANSI)
			gotGroup := tt.entry.CleanGroup(tt.stripANSI)

			if gotContent != tt.wantContent {
				t.Errorf("CleanContent() = %q, want %q", gotContent, tt.wantContent)
			}
			if gotGroup != tt.wantGroup {
				t.Errorf("CleanGroup() = %q, want %q", gotGroup, tt.wantGroup)
			}
		})
	}
}
