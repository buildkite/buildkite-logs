package buildkitelogs

import (
	"os"
	"testing"
	"time"
)

func TestParquetReader(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	t.Run("NewParquetReader", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		if reader == nil {
			t.Fatal("NewParquetReader returned nil")
		}
		if reader.filename != testFile {
			t.Errorf("Expected filename %s, got %s", testFile, reader.filename)
		}
	})

	t.Run("ReadEntriesIter", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		entryCount := 0
		var firstEntry ParquetLogEntry

		for entry, err := range reader.ReadEntriesIter() {
			if err != nil {
				t.Fatalf("ReadEntriesIter failed: %v", err)
			}

			if entryCount == 0 {
				firstEntry = entry
			}
			entryCount++

			// Stop after reading a few entries to verify streaming works
			if entryCount >= 10 {
				break
			}
		}

		if entryCount == 0 {
			t.Fatal("No entries read from Parquet file")
		}

		// Verify structure of first entry
		if firstEntry.Timestamp == 0 {
			t.Error("Expected non-zero timestamp")
		}
		if firstEntry.Content == "" {
			t.Error("Expected non-empty content")
		}
	})

	t.Run("FilterByGroupIter", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		entryCount := 0

		for entry, err := range reader.FilterByGroupIter("environment") {
			if err != nil {
				t.Fatalf("FilterByGroupIter failed: %v", err)
			}

			entryCount++

			// Verify all returned entries match the filter
			if entry.Group != "" && !containsIgnoreCase(entry.Group, "environment") {
				t.Errorf("Entry group '%s' does not contain 'environment'", entry.Group)
			}

			// Stop after reading a few entries
			if entryCount >= 5 {
				break
			}
		}

		// Note: We don't require finding entries since test data may not have "environment" groups
		t.Logf("Found %d entries matching 'environment'", entryCount)
	})

	t.Run("StreamingGroupAnalysis", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		groupMap := make(map[string]*GroupInfo)
		totalEntries := 0

		for entry, err := range reader.ReadEntriesIter() {
			if err != nil {
				t.Fatalf("ReadEntriesIter failed: %v", err)
			}

			totalEntries++

			groupName := entry.Group
			if groupName == "" {
				groupName = "<no group>"
			}

			info, exists := groupMap[groupName]
			if !exists {
				entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
				info = &GroupInfo{
					Name:      groupName,
					FirstSeen: entryTime,
					LastSeen:  entryTime,
				}
				groupMap[groupName] = info
			}

			info.EntryCount++
			if entry.IsCommand() {
				info.Commands++
			}

			// Stop after processing some entries for test performance
			if totalEntries >= 100 {
				break
			}
		}

		if len(groupMap) == 0 {
			t.Fatal("No groups found")
		}

		// Verify group structure
		for _, group := range groupMap {
			if group.Name == "" {
				t.Error("Expected non-empty group name")
			}
			if group.EntryCount == 0 {
				t.Error("Expected non-zero entry count")
			}
			if group.FirstSeen.IsZero() {
				t.Error("Expected non-zero FirstSeen time")
			}
			if group.LastSeen.IsZero() {
				t.Error("Expected non-zero LastSeen time")
			}
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		targetCount := 5
		actualCount := 0

		for _, err := range reader.ReadEntriesIter() {
			if err != nil {
				t.Fatalf("ReadEntriesIter failed: %v", err)
			}

			actualCount++
			if actualCount >= targetCount {
				break // Test early termination
			}
		}

		if actualCount != targetCount {
			t.Errorf("Expected to process exactly %d entries, got %d", targetCount, actualCount)
		}
	})
}

func TestStreamingGroupAnalysis(t *testing.T) {
	// Create test data
	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC).UnixMilli()
	testEntries := []ParquetLogEntry{
		{
			Timestamp: baseTime,
			Content:   "~~~ Running tests",
			Group:     "~~~ Running tests",
			Flags:     LogFlags(1 << IsGroup),
		},
		{
			Timestamp: baseTime + 100,
			Content:   "$ npm test",
			Group:     "~~~ Running tests",
			Flags:     LogFlags(1 << IsCommand),
		},
		{
			Timestamp: baseTime + 1000,
			Content:   "--- Build complete",
			Group:     "--- Build complete",
			Flags:     LogFlags(1 << IsGroup),
		},
	}

	// Simulate streaming group analysis
	groupMap := make(map[string]*GroupInfo)

	for _, entry := range testEntries {
		groupName := entry.Group
		if groupName == "" {
			groupName = "<no group>"
		}

		info, exists := groupMap[groupName]
		if !exists {
			entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
			info = &GroupInfo{
				Name:      groupName,
				FirstSeen: entryTime,
				LastSeen:  entryTime,
			}
			groupMap[groupName] = info
		}

		info.EntryCount++
		entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
		if entryTime.Before(info.FirstSeen) {
			info.FirstSeen = entryTime
		}
		if entryTime.After(info.LastSeen) {
			info.LastSeen = entryTime
		}

		if entry.IsCommand() {
			info.Commands++
		}
	}

	if len(groupMap) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(groupMap))
	}

	// Check first group
	testsGroup := groupMap["~~~ Running tests"]
	if testsGroup == nil {
		t.Fatal("Expected to find '~~~ Running tests' group")
	}
	if testsGroup.EntryCount != 2 {
		t.Errorf("Expected entry count 2, got %d", testsGroup.EntryCount)
	}
	if testsGroup.Commands != 1 {
		t.Errorf("Expected 1 command, got %d", testsGroup.Commands)
	}
}

func TestFilterByGroupIter_Standalone(t *testing.T) {
	testEntries := []ParquetLogEntry{
		{
			Content: "Environment setup",
			Group:   "~~~ Running global environment hook",
		},
		{
			Content: "Test command",
			Group:   "~~~ Running tests",
		},
		{
			Content: "Another environment task",
			Group:   "~~~ Pre-environment cleanup",
		},
	}

	// Create a streaming iterator from slice
	entryIter := func(yield func(ParquetLogEntry, error) bool) {
		for _, entry := range testEntries {
			if !yield(entry, nil) {
				return
			}
		}
	}

	filtered := make([]ParquetLogEntry, 0)
	for entry, err := range FilterByGroupIter(entryIter, "environment") {
		if err != nil {
			t.Fatalf("FilterByGroupIter failed: %v", err)
		}
		filtered = append(filtered, entry)
	}

	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered entries, got %d", len(filtered))
	}

	// Verify the correct entries were filtered
	expectedContents := []string{"Environment setup", "Another environment task"}
	for i, entry := range filtered {
		if entry.Content != expectedContents[i] {
			t.Errorf("Expected content '%s', got '%s'", expectedContents[i], entry.Content)
		}
	}
}

func TestReadParquetFileIter(t *testing.T) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	entryCount := 0
	var firstEntry ParquetLogEntry

	for entry, err := range ReadParquetFileIter(testFile) {
		if err != nil {
			t.Fatalf("ReadParquetFileIter failed: %v", err)
		}

		if entryCount == 0 {
			firstEntry = entry
		}
		entryCount++

		// Stop after reading some entries for test performance
		if entryCount >= 50 {
			break
		}
	}

	if entryCount == 0 {
		t.Fatal("No entries read from Parquet file")
	}

	// Verify first entry has expected structure
	if firstEntry.Timestamp == 0 {
		t.Error("Expected non-zero timestamp")
	}
	if firstEntry.Content == "" {
		t.Error("Expected non-empty content")
	}
}

func TestReadParquetFileIterNotFound(t *testing.T) {
	entryCount := 0
	for _, err := range ReadParquetFileIter("nonexistent.parquet") {
		if err != nil {
			// Expected to get an error on the first iteration
			return
		}
		entryCount++
	}

	if entryCount > 0 {
		t.Error("Expected error for non-existent file, but got entries")
	}
}

func TestStreamingPerformance(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test data not found")
	}

	reader := NewParquetReader(testFile)

	// Test that we can process entries without loading everything into memory
	t.Run("MemoryEfficient", func(t *testing.T) {
		entryCount := 0
		for _, err := range reader.ReadEntriesIter() {
			if err != nil {
				t.Fatalf("ReadEntriesIter failed: %v", err)
			}
			entryCount++

			// Process many entries to test memory efficiency
			if entryCount >= 1000 {
				break
			}
		}

		t.Logf("Successfully processed %d entries with streaming", entryCount)
	})

	// Test early termination performance
	t.Run("EarlyTermination", func(t *testing.T) {
		targetCount := 10
		actualCount := 0

		for _, err := range reader.ReadEntriesIter() {
			if err != nil {
				t.Fatalf("ReadEntriesIter failed: %v", err)
			}
			actualCount++
			if actualCount >= targetCount {
				break
			}
		}

		if actualCount != targetCount {
			t.Errorf("Expected exactly %d entries, got %d", targetCount, actualCount)
		}
	})
}

// Helper function for case-insensitive string contains check
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		len(substr) > 0 &&
		(s == substr ||
			(len(s) > len(substr) &&
				(stringContains(toLowerCase(s), toLowerCase(substr)))))
}

func toLowerCase(s string) string {
	result := make([]rune, len([]rune(s)))
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			result[i] = r + 32
		} else {
			result[i] = r
		}
	}
	return string(result)
}

func stringContains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}

	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestReverseSearch(t *testing.T) {
	// Create a temporary parquet file with test data
	testFile := "test_reverse_search.parquet"
	defer os.Remove(testFile)

	// Test entries with known patterns
	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC).UnixMilli()
	testEntries := []ParquetLogEntry{
		{Timestamp: baseTime, Content: "setup phase started", Group: "setup", Flags: 0},
		{Timestamp: baseTime + 100, Content: "installing dependencies", Group: "setup", Flags: 0},
		{Timestamp: baseTime + 200, Content: "test phase started", Group: "test", Flags: 0},
		{Timestamp: baseTime + 300, Content: "running unit tests", Group: "test", Flags: 0},
		{Timestamp: baseTime + 400, Content: "test failed: assertion error", Group: "test", Flags: 0},
		{Timestamp: baseTime + 500, Content: "cleanup phase started", Group: "cleanup", Flags: 0},
		{Timestamp: baseTime + 600, Content: "removing temp files", Group: "cleanup", Flags: 0},
		{Timestamp: baseTime + 700, Content: "build finished", Group: "cleanup", Flags: 0},
	}

	// Write test data to parquet file
	if err := writeTestParquetFile(testFile, testEntries); err != nil {
		t.Fatalf("Failed to create test parquet file: %v", err)
	}

	reader := NewParquetReader(testFile)

	t.Run("ForwardSearch", func(t *testing.T) {
		options := SearchOptions{
			Pattern: "test.*",
			Reverse: false,
		}

		results := []SearchResult{}
		for result, err := range reader.SearchEntriesIter(options) {
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			results = append(results, result)
		}

		// Debug output
		t.Logf("Forward search found %d results:", len(results))
		for i, result := range results {
			t.Logf("  Result %d: Line %d: %s", i, result.LineNumber, result.Match.Content)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 matches in forward search, got %d", len(results))
		}

		// Expected matches: "test phase started", "running unit tests", "test failed"
		if len(results) > 0 && results[0].LineNumber != 2 {
			t.Errorf("Expected first match at line 2, got %d", results[0].LineNumber)
		}
	})

	t.Run("ReverseSearch", func(t *testing.T) {
		options := SearchOptions{
			Pattern: "test.*",
			Reverse: true,
		}

		results := []SearchResult{}
		for result, err := range reader.SearchEntriesIter(options) {
			if err != nil {
				t.Fatalf("Reverse search failed: %v", err)
			}
			results = append(results, result)
		}

		// Debug output
		t.Logf("Reverse search found %d results:", len(results))
		for i, result := range results {
			t.Logf("  Result %d: Line %d: %s", i, result.LineNumber, result.Match.Content)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 matches in reverse search, got %d", len(results))
		}

		// In reverse search, first result should be "test failed" (line 4)
		if len(results) > 0 && results[0].LineNumber != 4 {
			t.Errorf("Expected first reverse match at line 4, got %d", results[0].LineNumber)
		}
	})

	t.Run("ReverseSearchWithSeek", func(t *testing.T) {
		options := SearchOptions{
			Pattern:   "test.*",
			Reverse:   true,
			SeekStart: 3, // Start from line 3 and search backwards
		}

		results := []SearchResult{}
		for result, err := range reader.SearchEntriesIter(options) {
			if err != nil {
				t.Fatalf("Reverse search with seek failed: %v", err)
			}
			results = append(results, result)
		}

		// Debug output
		t.Logf("Reverse search with seek found %d results:", len(results))
		for i, result := range results {
			t.Logf("  Result %d: Line %d: %s", i, result.LineNumber, result.Match.Content)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 matches in reverse search with seek, got %d", len(results))
		}

		// Should find both "running unit tests" (line 3) and "test phase started" (line 2)
		// when starting from line 3 and searching backwards
		if len(results) > 0 && results[0].LineNumber != 3 {
			t.Errorf("Expected first reverse seek match at line 3, got %d", results[0].LineNumber)
		}
		if len(results) > 1 && results[1].LineNumber != 2 {
			t.Errorf("Expected second reverse seek match at line 2, got %d", results[1].LineNumber)
		}
	})

	t.Run("ReverseSearchWithContext", func(t *testing.T) {
		options := SearchOptions{
			Pattern:       "test failed",
			Reverse:       true,
			BeforeContext: 1,
			AfterContext:  1,
		}

		results := []SearchResult{}
		for result, err := range reader.SearchEntriesIter(options) {
			if err != nil {
				t.Fatalf("Reverse search with context failed: %v", err)
			}
			results = append(results, result)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 match in reverse search with context, got %d", len(results))
		}

		result := results[0]
		if result.LineNumber != 4 {
			t.Errorf("Expected match at line 4, got %d", result.LineNumber)
		}

		// Check context: in reverse search, "before" context comes from higher indices
		if len(result.BeforeContext) != 1 {
			t.Errorf("Expected 1 before context line, got %d", len(result.BeforeContext))
		}

		// Check context: in reverse search, "after" context comes from lower indices
		if len(result.AfterContext) != 1 {
			t.Errorf("Expected 1 after context line, got %d", len(result.AfterContext))
		}
	})
}

// writeTestParquetFile creates a parquet file with test data
func writeTestParquetFile(filename string, entries []ParquetLogEntry) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := NewParquetWriter(file)
	defer writer.Close()

	// Convert ParquetLogEntry to LogEntry format for writing
	logEntries := make([]*LogEntry, len(entries))
	for i, entry := range entries {
		logEntries[i] = &LogEntry{
			Timestamp: time.Unix(0, entry.Timestamp*int64(time.Millisecond)),
			Content:   entry.Content,
			RawLine:   []byte(entry.Content),
			Group:     entry.Group,
		}
	}

	return writer.WriteBatch(logEntries)
}
