package buildkitelogs

import (
	"regexp"
	"strings"
)

// ansiRegex matches ANSI escape sequences including:
// - Color codes: \x1b[0m, \x1b[31m, etc.
// - Complex sequences: \x1b[0;31;40m
// - OSC sequences: \x1b]0;title\x07
// - Other control sequences: \x1b[2J, \x1b[H, etc.
// - Incomplete sequences: \x1b[, \x1b[31, etc.
var ansiRegex = regexp.MustCompile(`\x1b(?:\[[0-9;:? ]*[a-zA-Z]?|\](?:[^\x07\x1b]*(?:\x07|\x1b\\))?|[PX^_](?:[^\x1b]*\x1b\\)?|.?)`)

// StripANSIRegex removes ANSI escape sequences from a string using regex
func StripANSIRegex(s string) string {
	return ansiRegex.ReplaceAllString(s, "")
}

// StripANSI removes ANSI escape sequences using strings.Builder for efficiency
func StripANSI(s string) string {
	if !strings.Contains(s, "\x1b") {
		return s // Fast path: no escape sequences
	}

	var builder strings.Builder
	builder.Grow(len(s)) // Pre-allocate capacity

	b := []byte(s)
	i := 0

	for i < len(b) {
		if b[i] == '\x1b' {
			// Found ESC character, determine sequence type
			i++ // Skip ESC

			if i >= len(b) {
				// Lone ESC at end of string, consume it
				break
			}

			switch b[i] {
			case '[':
				// CSI sequence: ESC[...letter
				i++ // Skip [
				for i < len(b) && ((b[i] >= '0' && b[i] <= '9') || b[i] == ';' || b[i] == ':' || b[i] == '?' || b[i] == ' ') {
					i++
				}
				if i < len(b) && ((b[i] >= 'A' && b[i] <= 'Z') || (b[i] >= 'a' && b[i] <= 'z')) {
					i++ // Skip terminating letter
				}
				// If we hit end of string or invalid char, sequence is incomplete but consumed
			case ']':
				// OSC sequence: ESC]...BEL or ESC]...ESC\
				i++ // Skip ]
				for i < len(b) {
					if b[i] == '\x07' { // BEL
						i++
						break
					} else if b[i] == '\x1b' && i+1 < len(b) && b[i+1] == '\\' {
						i += 2 // Skip ESC\
						break
					}
					i++
				}
			case 'P', 'X', '^', '_':
				// DCS, SOS, PM, APC sequences: ESC{char}...ESC\
				i++ // Skip command char
				for i < len(b) {
					if b[i] == '\x1b' && i+1 < len(b) && b[i+1] == '\\' {
						i += 2 // Skip ESC\
						break
					}
					i++
				}
			default:
				// Simple escape sequence (Fe commands) or lone ESC, just skip the character
				i++
			}
		} else {
			builder.WriteByte(b[i])
			i++
		}
	}

	return builder.String()
}
