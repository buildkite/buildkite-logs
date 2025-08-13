package buildkitelogs

import (
	"bufio"
	"io"
	"iter"
	"strings"
	"time"
)

const (
	defaultBufferSize = 64 * 1024
	maxBufferSize     = 1024 * 1024
)

// LogEntry represents a parsed Buildkite log entry
type LogEntry struct {
	Timestamp time.Time
	Content   string // Parsed content after OSC processing, may still contain ANSI codes
	RawLine   []byte // Original line bytes including all OSC sequences and formatting
	Group     string // The current section/group this entry belongs to
}

// Parser handles parsing of Buildkite log files
type Parser struct {
	byteParser   *ByteParser
	currentGroup string
}

// LogIterator provides an iterator interface for processing log entries
type LogIterator struct {
	scanner *bufio.Scanner
	parser  *Parser
	current *LogEntry
	err     error
}

// NewParser creates a new Buildkite log parser
func NewParser() *Parser {
	return &Parser{
		byteParser: NewByteParser(),
	}
}

// Reset clears the parser's internal state, useful for reusing the parser
// for multiple independent parsing operations
func (p *Parser) Reset() {
	p.currentGroup = ""
}

// ParseLine parses a single log line
func (p *Parser) ParseLine(line string) (*LogEntry, error) {
	entry, err := p.byteParser.ParseLine(line)
	if err != nil {
		return nil, err
	}

	// Update current group if this is a group header
	if entry.IsGroup() {
		p.currentGroup = entry.Content
	}

	// Set the group for this entry
	entry.Group = p.currentGroup

	return entry, nil
}

// configureScanner configures a bufio.Scanner with appropriate buffer settings
// for handling potentially very long log lines
func configureScanner(scanner *bufio.Scanner) {
	// Set a large buffer to handle very long log lines (default is 64KB, set to 1MB)
	// see https://pkg.go.dev/bufio#MaxScanTokenSize
	scanner.Buffer(make([]byte, 0, defaultBufferSize), maxBufferSize)
}

// NewIterator creates a new LogIterator for memory-efficient processing
func (p *Parser) NewIterator(reader io.Reader) *LogIterator {
	scanner := bufio.NewScanner(reader)
	configureScanner(scanner)
	return &LogIterator{
		scanner: scanner,
		parser:  p,
	}
}

// All returns an iterator over all log entries using Go 1.23+ iter.Seq2 pattern
// Each iteration yields a *LogEntry and an error, following Go's idiomatic error handling
// This method creates isolated parser state to prevent contamination between iterations
func (p *Parser) All(reader io.Reader) iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		scanner := bufio.NewScanner(reader)
		configureScanner(scanner)
		// Create isolated parser state for this iteration to prevent state contamination
		localCurrentGroup := ""

		for scanner.Scan() {
			line := scanner.Text()
			// Parse line using byte parser directly to avoid state contamination
			entry, err := p.byteParser.ParseLine(line)
			if err != nil {
				if !yield(entry, err) {
					return
				}
				continue
			}

			// Handle group tracking with local state
			if entry.IsGroup() {
				localCurrentGroup = entry.Content
			}
			entry.Group = localCurrentGroup

			// Yield the processed entry
			if !yield(entry, err) {
				return
			}
		}

		// Check for scanner errors and yield final error if any
		if err := scanner.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// Next advances the iterator to the next log entry
// Returns true if there is a next entry, false if EOF or error
func (iter *LogIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	if !iter.scanner.Scan() {
		iter.err = iter.scanner.Err()
		return false
	}

	line := iter.scanner.Text()
	entry, err := iter.parser.ParseLine(line)
	if err != nil {
		iter.err = err
		return false
	}

	iter.current = entry
	return true
}

// Entry returns the current log entry
// Only valid after a successful call to Next()
func (iter *LogIterator) Entry() *LogEntry {
	return iter.current
}

// Err returns any error encountered during iteration
func (iter *LogIterator) Err() error {
	return iter.err
}

// HasTimestamp returns true if the log entry has a valid timestamp
func (entry *LogEntry) HasTimestamp() bool {
	return !entry.Timestamp.IsZero()
}

// IsGroup returns true if the log entry appears to be a group header
func (entry *LogEntry) IsGroup() bool {
	return strings.HasPrefix(entry.Content, "~~~ ") || strings.HasPrefix(entry.Content, "--- ") || strings.HasPrefix(entry.Content, "+++ ")
}

// IsSection is deprecated, use IsGroup instead
func (entry *LogEntry) IsSection() bool {
	return entry.IsGroup()
}

// ComputeFlags returns the consolidated flags for this log entry
func (entry *LogEntry) ComputeFlags() LogFlags {
	var flags LogFlags
	if entry.HasTimestamp() {
		flags.Set(HasTimestamp)
	}
	if entry.IsGroup() {
		flags.Set(IsGroup)
	}
	return flags
}
