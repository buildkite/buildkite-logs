package main

import (
	"strings"
	"testing"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func TestQueryConfigStripANSI(t *testing.T) {
	// Test that QueryConfig includes the StripANSI field
	config := QueryConfig{
		StripANSI: true,
	}

	if !config.StripANSI {
		t.Error("StripANSI field should be true")
	}
}

func TestFormatLogEntriesWithStripANSI(t *testing.T) {
	entries := []buildkitelogs.ParquetLogEntry{
		{
			Timestamp: 1640995200000, // 2022-01-01 00:00:00
			Content:   "\x1b[31mError: test failed\x1b[0m",
			Group:     "\x1b[32mTest Group\x1b[0m",
		},
		{
			Timestamp: 1640995200000,
			Content:   "Normal log line without ANSI",
			Group:     "Normal Group",
		},
	}

	// Test with StripANSI disabled
	config := &QueryConfig{
		RawOutput: true,
		StripANSI: false,
	}

	// Verify content is not modified when StripANSI is false
	for _, entry := range entries {
		actualContent := entry.CleanContent(config.StripANSI)
		actualGroup := entry.CleanGroup(config.StripANSI)

		if config.StripANSI == false && strings.Contains(entry.Content, "\x1b[") {
			if !strings.Contains(actualContent, "\x1b[") {
				t.Errorf("ANSI codes should be preserved when StripANSI=false. Got %q", actualContent)
			}
		}

		if config.StripANSI == false && strings.Contains(entry.Group, "\x1b[") {
			if !strings.Contains(actualGroup, "\x1b[") {
				t.Errorf("ANSI codes should be preserved in group when StripANSI=false. Got %q", actualGroup)
			}
		}
	}

	// Test with StripANSI enabled
	config.StripANSI = true

	// Verify content is modified when StripANSI is true
	for _, entry := range entries {
		actualContent := entry.CleanContent(config.StripANSI)
		actualGroup := entry.CleanGroup(config.StripANSI)

		if strings.Contains(actualContent, "\x1b[") {
			t.Errorf("ANSI codes should be stripped from content when StripANSI=true. Got %q", actualContent)
		}

		if strings.Contains(actualGroup, "\x1b[") {
			t.Errorf("ANSI codes should be stripped from group when StripANSI=true. Got %q", actualGroup)
		}

		// Test that whitespace is also trimmed
		if strings.HasPrefix(actualContent, " ") || strings.HasSuffix(actualContent, " ") {
			t.Errorf("Content should be trimmed. Got %q", actualContent)
		}

		if strings.HasPrefix(actualGroup, " ") || strings.HasSuffix(actualGroup, " ") {
			t.Errorf("Group should be trimmed. Got %q", actualGroup)
		}
	}
}

func TestStripANSIIntegration(t *testing.T) {
	// Test that the ANSI stripping integration works correctly
	testContent := "\x1b[31;1mError:\x1b[0m \x1b[31mtest failed\x1b[0m"
	expectedContent := "Error: test failed"

	actualContent := buildkitelogs.StripANSI(testContent)

	if actualContent != expectedContent {
		t.Errorf("StripANSI() = %q, want %q", actualContent, expectedContent)
	}
}
