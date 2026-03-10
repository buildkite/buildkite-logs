package buildkitelogs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParquetWriter(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_writer.parquet")
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewParquetWriter(file)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	entries := []*LogEntry{
		{
			Timestamp: time.Unix(0, 1745322209921*int64(time.Millisecond)),
			Content:   "test content",
			RawLine:   []byte("test raw line"),
			Group:     "test group",
		},
	}

	if err := writer.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}

func TestParquetWriterEmptyBatch(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_empty.parquet")
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewParquetWriter(file)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Writing an empty batch should be a no-op
	if err := writer.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil) error = %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestParquetWriterMultipleBatches(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_multi.parquet")
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewParquetWriter(file)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC)

	for batch := 0; batch < 3; batch++ {
		entries := make([]*LogEntry, 5)
		for i := range entries {
			entries[i] = &LogEntry{
				Timestamp: baseTime.Add(time.Duration(batch*5+i) * time.Millisecond),
				Content:   "line",
				Group:     "group",
			}
		}
		if err := writer.WriteBatch(entries); err != nil {
			t.Fatalf("WriteBatch(%d) error = %v", batch, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify all 15 entries round-trip
	reader := NewParquetReader(filename)
	var count int
	for _, err := range reader.ReadEntriesIter() {
		if err != nil {
			t.Fatalf("ReadEntriesIter error: %v", err)
		}
		count++
	}
	if count != 15 {
		t.Errorf("Expected 15 entries, got %d", count)
	}
}

func TestParquetRoundtrip(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_roundtrip.parquet")

	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC)
	entries := []*LogEntry{
		{Timestamp: baseTime, Content: "~~~ Setup group", RawLine: []byte("~~~ Setup group"), Group: "setup"},
		{Timestamp: baseTime.Add(100 * time.Millisecond), Content: "second line", RawLine: []byte("second line"), Group: "test"},
		{Timestamp: baseTime.Add(200 * time.Millisecond), Content: "third line", RawLine: []byte("third line"), Group: "cleanup"},
	}

	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewParquetWriter(file)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
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

		wantFlags := entry.ComputeFlags()
		if got.Flags != wantFlags {
			t.Errorf("Entry %d: flags = %d, want %d", i, got.Flags, wantFlags)
		}
	}

	// Verify specific flags: entry 0 has timestamp+group, entries 1-2 have timestamp only
	if !results[0].HasTime() {
		t.Error("Entry 0 should have timestamp flag")
	}
	if !results[0].IsGroup() {
		t.Error("Entry 0 should have group flag (starts with ~~~)")
	}
	if results[1].IsGroup() {
		t.Error("Entry 1 should not have group flag")
	}
}

func TestParquetRoundtripRowNumbers(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_rows.parquet")

	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC)
	entries := make([]*LogEntry, 10)
	for i := range entries {
		entries[i] = &LogEntry{
			Timestamp: baseTime.Add(time.Duration(i) * time.Millisecond),
			Content:   "line",
			Group:     "g",
		}
	}

	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	writer, err := NewParquetWriter(file)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	if err := writer.WriteBatch(entries); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reader := NewParquetReader(testFile)
	var rowNumbers []int64
	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			t.Fatalf("ReadEntriesIter failed: %v", err)
		}
		rowNumbers = append(rowNumbers, entry.RowNumber)
	}

	for i, rn := range rowNumbers {
		if rn != int64(i) {
			t.Errorf("Entry %d: RowNumber = %d, want %d", i, rn, i)
		}
	}
}

func TestParquetSeq2Export(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output"

	filename := filepath.Join(t.TempDir(), "test_seq2.parquet")
	if err := ExportSeq2ToParquet(parser.All(strings.NewReader(testData)), filename); err != nil {
		t.Fatalf("ExportSeq2ToParquet() error = %v", err)
	}

	// Verify content round-trips correctly
	var results []ParquetLogEntry
	for entry, err := range ReadParquetFileIter(filename) {
		if err != nil {
			t.Fatalf("ReadParquetFileIter error: %v", err)
		}
		results = append(results, entry)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(results))
	}
	if results[0].Content != "~~~ Running global environment hook" {
		t.Errorf("Entry 0 content = %q", results[0].Content)
	}
	if results[2].Content != "Some regular output" {
		t.Errorf("Entry 2 content = %q", results[2].Content)
	}
}

func TestParquetSeq2ExportWithFilter(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output\n" +
		"\x1b_bk;t=1745322209924\x07$ git clone repo"

	filename := filepath.Join(t.TempDir(), "test_seq2_filtered.parquet")

	filterFunc := func(entry *LogEntry) bool {
		return strings.Contains(entry.Content, "$")
	}

	if err := ExportSeq2ToParquetWithFilter(parser.All(strings.NewReader(testData)), filename, filterFunc); err != nil {
		t.Fatalf("ExportSeq2ToParquetWithFilter() error = %v", err)
	}

	// Verify only filtered entries were written
	var results []ParquetLogEntry
	for entry, err := range ReadParquetFileIter(filename) {
		if err != nil {
			t.Fatalf("ReadParquetFileIter error: %v", err)
		}
		results = append(results, entry)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 filtered entries, got %d", len(results))
	}
	for _, r := range results {
		if !strings.Contains(r.Content, "$") {
			t.Errorf("Filtered entry should contain '$', got %q", r.Content)
		}
	}
}
