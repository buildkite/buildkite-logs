// Package logparser parses Buildkite job logs into structured entries.
//
// It handles Buildkite timestamp OSC sequences, group tracking, large-line
// limits, optional truncation, and structured parse errors for malformed input.
package logparser
