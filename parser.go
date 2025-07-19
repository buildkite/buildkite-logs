package buildkitelogs

import (
	"bufio"
	"io"
	"iter"
	"strings"
	"time"
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

// ParseLine parses a single log line
func (p *Parser) ParseLine(line string) (*LogEntry, error) {
	entry, err := p.byteParser.ParseLine(line)
	if err != nil {
		return nil, err
	}

	// Update current group if this is a group header
	if entry.IsGroup() {
		p.currentGroup = entry.CleanContent()
	}

	// Set the group for this entry
	entry.Group = p.currentGroup

	return entry, nil
}

// NewIterator creates a new LogIterator for memory-efficient processing
func (p *Parser) NewIterator(reader io.Reader) *LogIterator {
	return &LogIterator{
		scanner: bufio.NewScanner(reader),
		parser:  p,
	}
}

// All returns an iterator over all log entries using Go 1.23+ iter.Seq2 pattern
// Each iteration yields a *LogEntry and an error, following Go's idiomatic error handling
func (p *Parser) All(reader io.Reader) iter.Seq2[*LogEntry, error] {
	return func(yield func(*LogEntry, error) bool) {
		scanner := bufio.NewScanner(reader)

		for scanner.Scan() {
			line := scanner.Text()
			entry, err := p.ParseLine(line)

			// Yield both the entry (which may be nil if err != nil) and the error
			if !yield(entry, err) {
				return
			}

			// If there was a parse error, we've yielded it to the caller
			// They can decide whether to continue or stop
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

// StripANSI removes ANSI escape sequences from content
func (p *Parser) StripANSI(content string) string {
	return p.byteParser.StripANSI(content)
}

// CleanContent returns the content with ANSI codes stripped
func (entry *LogEntry) CleanContent() string {
	parser := NewParser()
	return parser.StripANSI(entry.Content)
}

// HasTimestamp returns true if the log entry has a valid timestamp
func (entry *LogEntry) HasTimestamp() bool {
	return !entry.Timestamp.IsZero()
}

// IsCommand returns true if the log entry appears to be a command execution
func (entry *LogEntry) IsCommand() bool {
	clean := entry.CleanContent()
	return strings.HasPrefix(clean, "$ ")
}

// IsGroup returns true if the log entry appears to be a group header
func (entry *LogEntry) IsGroup() bool {
	content := entry.CleanContent()
	return strings.HasPrefix(content, "~~~") || strings.HasPrefix(content, "---") || strings.HasPrefix(content, "+++")
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
	if entry.IsCommand() {
		flags.Set(IsCommand)
	}
	if entry.IsGroup() {
		flags.Set(IsGroup)
	}
	return flags
}
