package buildkitelogs

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParquetWriter(t *testing.T) {
	// Create test file
	filename := "test_writer.parquet"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer func() {
		// It's okay if the file doesn't exist or can't be removed in tests
		_ = os.Remove(filename)
	}()

	// Create writer
	writer := NewParquetWriter(file)
	defer func() {
		if err := writer.Close(); err != nil {
			t.Logf("Warning: failed to close writer: %v", err)
		}
	}()

	// Create test entries
	entries := []*LogEntry{
		{
			Timestamp: time.Unix(0, 1745322209921*int64(time.Millisecond)),
			Content:   "test content",
			RawLine:   []byte("test raw line"),
			Group:     "test group",
		},
	}

	// Write batch
	err = writer.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	// Close writer and file
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Check file was written
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}

func TestParquetRoundtrip(t *testing.T) {
	testFile := "test_roundtrip.parquet"
	defer os.Remove(testFile)

	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC)
	entries := []*LogEntry{
		{Timestamp: baseTime, Content: "first line", RawLine: []byte("first line"), Group: "setup"},
		{Timestamp: baseTime.Add(100 * time.Millisecond), Content: "second line", RawLine: []byte("second line"), Group: "test"},
		{Timestamp: baseTime.Add(200 * time.Millisecond), Content: "third line", RawLine: []byte("third line"), Group: "cleanup"},
	}

	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer := NewParquetWriter(file)
	if err := writer.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reader := NewParquetReader(testFile)
	var results []ParquetLogEntry
	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			t.Fatalf("ReadEntriesIter failed: %v", err)
		}
		results = append(results, entry)
	}

	if len(results) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(results))
	}

	for i, entry := range entries {
		got := results[i]
		wantTS := entry.Timestamp.UnixMilli()
		if got.Timestamp != wantTS {
			t.Errorf("Entry %d: timestamp = %d, want %d", i, got.Timestamp, wantTS)
		}
		if got.Content != entry.Content {
			t.Errorf("Entry %d: content = %q, want %q", i, got.Content, entry.Content)
		}
		if got.Group != entry.Group {
			t.Errorf("Entry %d: group = %q, want %q", i, got.Group, entry.Group)
		}
	}
}

func TestParquetSeq2Export(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output"

	reader := strings.NewReader(testData)

	// Export using Seq2
	filename := "test_seq2_output.parquet"
	err := ExportSeq2ToParquet(parser.All(reader), filename)
	if err != nil {
		t.Fatalf("ExportSeq2ToParquet() error = %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("Parquet file was not created")
	}

	// Cleanup
	defer func() {
		// It's okay if the file doesn't exist or can't be removed in tests
		_ = os.Remove(filename)
	}()

	// Check file is not empty
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}

func TestParquetSeq2ExportWithFilter(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output\n" +
		"\x1b_bk;t=1745322209924\x07$ git clone repo"

	reader := strings.NewReader(testData)

	// Filter for entries containing '$'
	filterFunc := func(entry *LogEntry) bool {
		return strings.Contains(entry.Content, "$")
	}

	// Export using Seq2 with filter
	filename := "test_seq2_filtered.parquet"
	err := ExportSeq2ToParquetWithFilter(parser.All(reader), filename, filterFunc)
	if err != nil {
		t.Fatalf("ExportSeq2ToParquetWithFilter() error = %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("Parquet file was not created")
	}

	// Cleanup
	defer func() {
		// It's okay if the file doesn't exist or can't be removed in tests
		_ = os.Remove(filename)
	}()

	// Check file is not empty (should contain 2 entries with '$')
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}
