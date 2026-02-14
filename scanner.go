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
		// OSC framing present but timestamp unparseable — treat as untimed line
		// Strip the OSC envelope but preserve content after BEL
		contentStart := timestampEnd + 1
		contentData := data[contentStart:]

		contentEnd := findNextOSCStart(contentData)
		if contentEnd != -1 {
			contentData = contentData[:contentEnd]
		}

		return &LogEntry{
			Timestamp: time.Time{},
			Content:   string(contentData),
			RawLine:   data,
			Group:     "",
		}, nil
	}

	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))

	// Extract content (after BEL)
	contentStart := timestampEnd + 1
	contentData := data[contentStart:]

	// For single OSC sequence model: truncate content at first subsequent OSC sequence
	// Look for next \x1b_bk;t= pattern in the content
	contentEnd := findNextOSCStart(contentData)
	if contentEnd != -1 {
		// Truncate at the next OSC sequence to keep only the first content piece
		contentData = contentData[:contentEnd]
	}

	content := string(contentData)

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

// findNextOSCStart finds the next OSC sequence (\x1b_bk;t=) in the data
// Returns -1 if no subsequent OSC sequence is found
func findNextOSCStart(data []byte) int {
	oscPattern := []byte{0x1b, '_', 'b', 'k', ';', 't', '='}

	for i := 0; i <= len(data)-len(oscPattern); i++ {
		if bytes.Equal(data[i:i+len(oscPattern)], oscPattern) {
			return i
		}
	}
	return -1
}
