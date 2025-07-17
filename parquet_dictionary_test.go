package buildkitelogs

import (
	"os"
	"testing"
	"time"
)

func TestDictionaryEncodingAcrossBatches(t *testing.T) {
	// Create test data with repeated strings across batches
	entry1 := &LogEntry{
		Timestamp: time.Now(),
		Content:   "repeated content",
		Group:     "common group",
	}
	entry2 := &LogEntry{
		Timestamp: time.Now(),
		Content:   "different content",
		Group:     "common group", // This will be repeated
	}
	entry3 := &LogEntry{
		Timestamp: time.Now(),
		Content:   "repeated content", // This will be repeated from batch 1
		Group:     "another group",
	}

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "dictionary_test_*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Create writer
	writer := NewParquetWriter(tmpFile)
	if writer == nil {
		t.Fatal("Failed to create ParquetWriter")
	}

	// Write first batch
	batch1 := []*LogEntry{entry1, entry2}
	if err := writer.WriteBatch(batch1); err != nil {
		t.Fatalf("Failed to write batch 1: %v", err)
	}

	// Write second batch with some repeated strings
	batch2 := []*LogEntry{entry3}
	if err := writer.WriteBatch(batch2); err != nil {
		t.Fatalf("Failed to write batch 2: %v", err)
	}

	// Close writer (this will also close the file)
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify file size - dictionary encoding should make it smaller
	// than if we used regular string encoding
	stat, err := os.Stat(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// The file should be successfully created and have content
	if stat.Size() == 0 {
		t.Error("Output file is empty")
	}

	t.Logf("Dictionary-encoded file size: %d bytes", stat.Size())

	// Test that we can read the file back
	var entries []ParquetLogEntry
	for entry, err := range ReadParquetFileIter(tmpFile.Name()) {
		if err != nil {
			t.Fatalf("Error reading entries: %v", err)
		}
		entries = append(entries, entry)
	}

	// Verify we got all entries back
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// Verify content was preserved
	if entries[0].Content != "repeated content" {
		t.Errorf("Entry 0 content mismatch: got %s, want %s", entries[0].Content, "repeated content")
	}
	if entries[1].Group != "common group" {
		t.Errorf("Entry 1 group mismatch: got %s, want %s", entries[1].Group, "common group")
	}
	if entries[2].Content != "repeated content" {
		t.Errorf("Entry 2 content mismatch: got %s, want %s", entries[2].Content, "repeated content")
	}
}
