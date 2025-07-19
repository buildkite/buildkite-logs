package buildkitelogs

import (
	"os"
	"testing"
	"time"
)

// BenchmarkParquetReader_ReadEntriesIter benchmarks the streaming ReadEntriesIter method
func BenchmarkParquetReader_ReadEntriesIter(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	reader := NewParquetReader(testFile)

	b.ReportAllocs()

	for b.Loop() {
		count := 0
		for entry, err := range reader.ReadEntriesIter() {
			if err != nil {
				b.Fatalf("ReadEntriesIter failed: %v", err)
			}
			count++
			_ = entry
		}
		if count == 0 {
			b.Fatal("No entries found")
		}
	}
}

// BenchmarkParquetReader_FilterByGroupIter benchmarks the streaming FilterByGroupIter method
func BenchmarkParquetReader_FilterByGroupIter(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	reader := NewParquetReader(testFile)

	b.ReportAllocs()

	for b.Loop() {
		count := 0
		for entry, err := range reader.FilterByGroupIter("environment") {
			if err != nil {
				b.Fatalf("FilterByGroupIter failed: %v", err)
			}
			count++
			_ = entry
		}
	}
}

// BenchmarkReadParquetFileIter benchmarks the standalone ReadParquetFileIter function
func BenchmarkReadParquetFileIter(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		count := 0
		for entry, err := range ReadParquetFileIter(testFile) {
			if err != nil {
				b.Fatalf("ReadParquetFileIter failed: %v", err)
			}
			count++
			_ = entry
		}
		if count == 0 {
			b.Fatal("No entries found")
		}
	}
}

// BenchmarkStreamingGroupAnalysis benchmarks building group statistics via streaming
func BenchmarkStreamingGroupAnalysis(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		groupMap := make(map[string]*GroupInfo)

		for entry, err := range reader.ReadEntriesIter() {
			if err != nil {
				b.Fatalf("ReadEntriesIter failed: %v", err)
			}

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
		}

		if len(groupMap) == 0 {
			b.Fatal("No groups found")
		}
	}
}

// BenchmarkParquetReaderOperations compares different streaming operations
func BenchmarkParquetReaderOperations(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("ReadEntriesIter", func(b *testing.B) {
		for b.Loop() {
			count := 0
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}
				count++
				_ = entry
			}
		}
	})

	b.Run("FilterByGroupIter", func(b *testing.B) {
		for b.Loop() {
			count := 0
			for entry, err := range reader.FilterByGroupIter("environment") {
				if err != nil {
					b.Fatalf("FilterByGroupIter failed: %v", err)
				}
				count++
				_ = entry
			}
		}
	})
}

// BenchmarkStreamingMemoryUsage tests memory allocation patterns for streaming operations
func BenchmarkStreamingMemoryUsage(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("StreamingProcessing", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}

				// Simulate processing entry
				_ = entry.Content
			}
		}
	})
}

// BenchmarkEarlyTermination tests iterator advantage for partial processing
func BenchmarkEarlyTermination(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)
	targetCount := 100 // Only process first 100 entries

	b.Run("Iterator_StopEarly", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			count := 0
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}

				_ = entry.Content
				count++
				if count >= targetCount {
					break // Iterator stops reading file
				}
			}
		}
	})

	b.Run("FilterIter_StopEarly", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			count := 0
			for entry, err := range reader.FilterByGroupIter("test") {
				if err != nil {
					b.Fatalf("FilterByGroupIter failed: %v", err)
				}

				_ = entry.Content
				count++
				if count >= 10 { // Stop after finding 10 test entries
					break
				}
			}
		}
	})
}

// BenchmarkFilterByGroupIter benchmarks the standalone FilterByGroupIter function
func BenchmarkFilterByGroupIter_Standalone(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		count := 0
		entries := ReadParquetFileIter(testFile)
		for entry, err := range FilterByGroupIter(entries, "test") {
			if err != nil {
				b.Fatalf("FilterByGroupIter failed: %v", err)
			}
			count++
			_ = entry
		}
	}
}

// BenchmarkStreamingVsChaining compares different streaming approaches
func BenchmarkStreamingVsChaining(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("DirectFiltering", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			count := 0
			for entry, err := range reader.FilterByGroupIter("environment") {
				if err != nil {
					b.Fatalf("FilterByGroupIter failed: %v", err)
				}
				count++
				_ = entry
			}
		}
	})

	b.Run("ChainedFiltering", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			count := 0
			entries := reader.ReadEntriesIter()
			for entry, err := range FilterByGroupIter(entries, "environment") {
				if err != nil {
					b.Fatalf("FilterByGroupIter failed: %v", err)
				}
				count++
				_ = entry
			}
		}
	})
}

// BenchmarkStreamingScalability tests performance with different processing patterns
func BenchmarkStreamingScalability(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("CountOnly", func(b *testing.B) {
		for b.Loop() {
			count := 0
			for _, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}
				count++
			}
		}
	})

	b.Run("SimpleProcessing", func(b *testing.B) {
		for b.Loop() {
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}
				// Simple processing: check content length
				_ = len(entry.Content)
			}
		}
	})

	b.Run("ComplexProcessing", func(b *testing.B) {
		for b.Loop() {
			groupStats := make(map[string]int)
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					b.Fatalf("ReadEntriesIter failed: %v", err)
				}
				// Complex processing: build statistics
				groupStats[entry.Group]++
				if entry.IsCommand() {
					groupStats[entry.Group+"_commands"]++
				}
			}
		}
	})
}
