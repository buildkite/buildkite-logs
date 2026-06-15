package logparser

import (
	"strings"
	"time"
)

// Entry represents a parsed Buildkite log entry.
type Entry struct {
	Timestamp time.Time
	Content   string // Parsed content after OSC processing, may still contain ANSI codes.
	RawLine   []byte // Parsed line bytes excluding the trailing newline; truncated lines include the suffix.
	Group     string // The current section/group this entry belongs to.
}

type LogFlag int32

const (
	HasTimestamp LogFlag = iota
	IsGroup
)

// LogFlags represents a bitwise combination of log flags.
type LogFlags int32

// Has returns true if the specified flag is set.
func (lf LogFlags) Has(flag LogFlag) bool {
	return lf&(1<<flag) != 0
}

// Set sets the specified flag.
func (lf *LogFlags) Set(flag LogFlag) {
	*lf |= (1 << flag)
}

// Clear clears the specified flag.
func (lf *LogFlags) Clear(flag LogFlag) {
	*lf &^= (1 << flag)
}

// Toggle toggles the specified flag.
func (lf *LogFlags) Toggle(flag LogFlag) {
	*lf ^= (1 << flag)
}

// HasTimestamp returns true if HasTimestamp flag is set.
func (lf LogFlags) HasTimestamp() bool {
	return lf.Has(HasTimestamp)
}

// IsGroup returns true if IsGroup flag is set.
func (lf LogFlags) IsGroup() bool {
	return lf.Has(IsGroup)
}

// HasTimestamp returns true if the log entry has a valid timestamp.
func (entry *Entry) HasTimestamp() bool {
	return !entry.Timestamp.IsZero()
}

// IsGroup returns true if the log entry appears to be a group header.
func (entry *Entry) IsGroup() bool {
	return strings.HasPrefix(entry.Content, "~~~ ") ||
		strings.HasPrefix(entry.Content, "--- ") ||
		strings.HasPrefix(entry.Content, "+++ ")
}

// IsSection is an alias for IsGroup.
func (entry *Entry) IsSection() bool {
	return entry.IsGroup()
}

// ComputeFlags returns the consolidated flags for this log entry.
func (entry *Entry) ComputeFlags() LogFlags {
	var flags LogFlags
	if entry.HasTimestamp() {
		flags.Set(HasTimestamp)
	}
	if entry.IsGroup() {
		flags.Set(IsGroup)
	}
	return flags
}
