package buildkitelogs

import (
	"context"
	"os"
	"testing"
)

func TestParquetReader_GetFileInfo(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test data not found")
	}

	reader := NewParquetReader(context.Background(), testFile)

	info, err := reader.GetFileInfo()
	if err != nil {
		t.Fatalf("GetFileInfo failed: %v", err)
	}

	if info.RowCount <= 0 {
		t.Error("Expected positive row count")
	}
	if info.ColumnCount <= 0 {
		t.Error("Expected positive column count")
	}
	if info.FileSize <= 0 {
		t.Error("Expected positive file size")
	}
	if info.NumRowGroups <= 0 {
		t.Error("Expected positive number of row groups")
	}

	t.Logf("File info: %d rows, %d columns, %d bytes, %d row groups",
		info.RowCount, info.ColumnCount, info.FileSize, info.NumRowGroups)
}

func TestParquetReader_SeekToRow(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test data not found")
	}

	reader := NewParquetReader(context.Background(), testFile)

	// Get file info first
	info, err := reader.GetFileInfo()
	if err != nil {
		t.Fatalf("GetFileInfo failed: %v", err)
	}

	t.Run("SeekToBeginning", func(t *testing.T) {
		entryCount := 0
		for entry, err := range reader.SeekToRow(0) {
			if err != nil {
				t.Fatalf("SeekToRow(0) failed: %v", err)
			}
			entryCount++

			// Verify we got an entry
			if entry.Content == "" && entry.Timestamp == 0 {
				t.Error("Expected valid entry content or timestamp")
			}

			// Stop after a few entries to verify seeking works
			if entryCount >= 5 {
				break
			}
		}

		if entryCount == 0 {
			t.Error("Expected to read some entries from beginning")
		}
	})

	t.Run("SeekToMiddle", func(t *testing.T) {
		if info.RowCount < 10 {
			t.Skip("File too small for middle seek test")
		}

		midRow := info.RowCount / 2
		entryCount := 0

		for entry, err := range reader.SeekToRow(midRow) {
			if err != nil {
				t.Fatalf("SeekToRow(%d) failed: %v", midRow, err)
			}
			entryCount++

			// Verify we got an entry
			if entry.Content == "" && entry.Timestamp == 0 {
				t.Error("Expected valid entry content or timestamp")
			}

			// Stop after a few entries
			if entryCount >= 3 {
				break
			}
		}

		if entryCount == 0 {
			t.Error("Expected to read some entries from middle")
		}
	})

	t.Run("SeekBeyondFile", func(t *testing.T) {
		entryCount := 0
		for _, err := range reader.SeekToRow(info.RowCount + 100) {
			if err == nil {
				entryCount++
				if entryCount > 0 {
					t.Error("Expected error when seeking beyond file bounds")
					break
				}
			} else {
				// Expected error case
				t.Logf("Got expected error: %v", err)
				break
			}
		}
	})

	t.Run("SeekWithEarlyTermination", func(t *testing.T) {
		entryCount := 0
		targetCount := 3

		for _, err := range reader.SeekToRow(5) {
			if err != nil {
				t.Fatalf("SeekToRow(5) failed: %v", err)
			}
			entryCount++

			if entryCount >= targetCount {
				break // Test early termination
			}
		}

		if entryCount != targetCount {
			t.Errorf("Expected exactly %d entries, got %d", targetCount, entryCount)
		}
	})
}

func TestReadParquetFileFromRowIter_Standalone(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test data not found")
	}

	// Test seeking to row 0
	entryCount := 0
	for entry, err := range readParquetFileFromRowIter(context.Background(), testFile, 0) {
		if err != nil {
			t.Fatalf("readParquetFileFromRowIter failed: %v", err)
		}

		entryCount++

		// Verify entry structure
		if entry.Content == "" && entry.Timestamp == 0 {
			t.Error("Expected valid entry content or timestamp")
		}

		// Stop after reading a few entries
		if entryCount >= 10 {
			break
		}
	}

	if entryCount == 0 {
		t.Error("Expected to read some entries")
	}
}

func TestGetParquetFileInfo_Standalone(t *testing.T) {
	testFile := "testdata/bash-example.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test data not found")
	}

	info, err := getParquetFileInfo(testFile)
	if err != nil {
		t.Fatalf("getParquetFileInfo failed: %v", err)
	}

	if info.RowCount <= 0 {
		t.Error("Expected positive row count")
	}
	if info.ColumnCount <= 0 {
		t.Error("Expected positive column count")
	}
	if info.FileSize <= 0 {
		t.Error("Expected positive file size")
	}
	if info.NumRowGroups <= 0 {
		t.Error("Expected positive number of row groups")
	}
}

func TestGetParquetFileInfo_NonExistent(t *testing.T) {
	_, err := getParquetFileInfo("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}
