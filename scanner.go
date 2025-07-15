package buildkitelogs

import (
	"bytes"
	"strconv"
	"time"
)

// ByteParser handles byte-level parsing of Buildkite log files
type ByteParser struct{}

// NewByteParser creates a new byte-based parser
func NewByteParser() *ByteParser {
	return &ByteParser{}
}

// ParseLine parses a single log line using byte scanning
func (p *ByteParser) ParseLine(line string) (*LogEntry, error) {
	data := []byte(line)

	// Check for OSC sequence: ESC_bk;t=timestamp BEL content
	if len(data) < 10 { // Minimum: \x1b_bk;t=1\x07
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Look for OSC start sequence: \x1b_bk;t=
	if !hasOSCStart(data) {
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Find the timestamp and content
	timestampStart := 7 // After \x1b_bk;t=
	timestampEnd := findBEL(data, timestampStart)
	if timestampEnd == -1 {
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Extract timestamp
	timestampBytes := data[timestampStart:timestampEnd]
	timestampMs, err := strconv.ParseInt(string(timestampBytes), 10, 64)
	if err != nil {
		return nil, err
	}

	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))

	// Extract content (after BEL)
	content := string(data[timestampEnd+1:])

	return &LogEntry{
		Timestamp: timestamp,
		Content:   content,
		RawLine:   data,
		Group:     "",
	}, nil
}

// hasOSCStart checks if data starts with \x1b_bk;t=
func hasOSCStart(data []byte) bool {
	if len(data) < 7 {
		return false
	}

	return data[0] == 0x1b && // ESC
		bytes.HasPrefix(data[1:], []byte("_bk;t="))
}

// findBEL finds the position of the BEL character (\x07) starting from offset
func findBEL(data []byte, start int) int {
	for i := start; i < len(data); i++ {
		if data[i] == 0x07 {
			return i
		}
	}
	return -1
}

// StripANSI removes ANSI escape sequences using byte scanning
func (p *ByteParser) StripANSI(content string) string {
	data := []byte(content)
	result := make([]byte, 0, len(data))

	i := 0
	for i < len(data) {
		// Try to handle ANSI escape sequence with ESC[
		if nextPos := p.skipESCSequence(data, i); nextPos > i {
			i = nextPos
			continue
		}

		// Try to handle sequences that might be missing ESC
		if nextPos := p.skipBareSequence(data, i); nextPos > i {
			i = nextPos
			continue
		}

		// Regular character - keep it
		result = append(result, data[i])
		i++
	}

	return string(result)
}

// skipESCSequence checks for and skips ESC[ ANSI sequences
// Returns the position after the sequence, or the original position if no sequence found
func (p *ByteParser) skipESCSequence(data []byte, pos int) int {
	if pos >= len(data)-1 {
		return pos
	}

	if data[pos] != 0x1b || data[pos+1] != '[' {
		return pos
	}

	// Skip ESC[
	i := pos + 2

	// Skip until we find the final character (letter)
	for i < len(data) && !isANSIFinalChar(data[i]) {
		i++
	}

	// Skip the final character
	if i < len(data) {
		i++
	}

	return i
}

// skipBareSequence checks for and skips bare [ sequences that look like ANSI
// Returns the position after the sequence, or the original position if no sequence found
func (p *ByteParser) skipBareSequence(data []byte, pos int) int {
	if pos >= len(data)-1 || data[pos] != '[' {
		return pos
	}

	// Look ahead to see if this looks like an ANSI sequence
	j := pos + 1
	for j < len(data) && j < pos+10 { // Limit lookahead
		if p.isANSIParam(data[j]) {
			j++
			continue
		}

		if isANSIFinalChar(data[j]) {
			return j + 1 // Skip the ANSI sequence
		}

		break // Not an ANSI sequence
	}

	return pos // No valid ANSI sequence found
}

// isANSIParam checks if a byte is a valid ANSI parameter character
func (p *ByteParser) isANSIParam(b byte) bool {
	return (b >= '0' && b <= '9') || b == ';'
}

// isANSIFinalChar checks if a byte is a valid ANSI sequence final character
func isANSIFinalChar(b byte) bool {
	// ANSI sequences end with letters, typically m, K, H, etc.
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}
