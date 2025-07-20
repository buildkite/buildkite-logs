package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func TestFormatLogEntries_RawOutput(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	entries := []buildkitelogs.ParquetLogEntry{
		{
			Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
			Content:   "Test content 1",
			Group:     "test-group",
			Flags:     buildkitelogs.LogFlags(1 << 1), // IsCommand (1 << IsCommand)
		},
		{
			Timestamp: time.Date(2025, 1, 1, 12, 1, 0, 0, time.UTC).UnixMilli(),
			Content:   "Test content 2",
			Group:     "",
			Flags:     buildkitelogs.LogFlags(1 << 2), // IsGroup (1 << IsGroup)
		},
	}

	config := &QueryConfig{RawOutput: true}

	formatLogEntries(entries, config)

	// Close writer and capture output
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("Failed to copy output: %v", err)
	}
	output := buf.String()

	expected := "Test content 1\nTest content 2\n"
	if output != expected {
		t.Errorf("Raw output mismatch.\nGot: %q\nWant: %q", output, expected)
	}
}

func TestFormatLogEntries_FormattedOutput(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	entries := []buildkitelogs.ParquetLogEntry{
		{
			Timestamp: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
			Content:   "Test content 1",
			Group:     "test-group",
			Flags:     buildkitelogs.LogFlags(1 << 1), // IsCommand (1 << IsCommand)
		},
		{
			Timestamp: time.Date(2025, 1, 1, 12, 1, 0, 0, time.UTC).UnixMilli(),
			Content:   "Group Title",
			Group:     "Group Title",                  // Same as content
			Flags:     buildkitelogs.LogFlags(1 << 2), // IsGroup (1 << IsGroup)
		},
	}

	config := &QueryConfig{RawOutput: false}

	formatLogEntries(entries, config)

	// Close writer and capture output
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		t.Fatalf("Failed to copy output: %v", err)
	}
	output := buf.String()

	// Should show timestamp, group, and CMD marker for first entry
	// Should show timestamp and GRP marker for second entry (no duplicate group name)
	if !strings.Contains(output, "[test-group] [CMD] Test content 1") {
		t.Errorf("First entry not formatted correctly. Got: %s", output)
	}
	if !strings.Contains(output, "[GRP] Group Title") {
		t.Errorf("Second entry not formatted correctly. Got: %s", output)
	}
	// Should not show duplicate group name
	if strings.Contains(output, "[Group Title] [GRP] Group Title") {
		t.Errorf("Group name should not be duplicated. Got: %s", output)
	}
	// Should contain expected content
	if !strings.Contains(output, "Test content 1") {
		t.Errorf("Missing first entry content. Got: %s", output)
	}
	if !strings.Contains(output, "Group Title") {
		t.Errorf("Missing second entry content. Got: %s", output)
	}
}
