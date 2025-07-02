package buildkitelogs

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// generateTestData creates synthetic log data for benchmarking
func generateTestData(numLines int) string {
	var builder strings.Builder

	entries := []string{
		"\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
		"\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment",
		"\x1b_bk;t=1745322209948\x07~~~ Running global pre-checkout hook",
		"\x1b_bk;t=1745322209949\x07[90m$[0m /buildkite/agent/hooks/pre-checkout",
		"\x1b_bk;t=1745322209975\x07~~~ Preparing working directory",
		"\x1b_bk;t=1745322209975\x07[90m# Creating \"/buildkite/builds/example\"[0m",
		"\x1b_bk;t=1745322209975\x07[90m$[0m cd /buildkite/builds/example",
		"\x1b_bk;t=1745322209975\x07[90m$[0m git clone -v -- https://github.com/example/repo.git .",
		"\x1b_bk;t=1745322209976\x07Cloning into '.'...",
		"\x1b_bk;t=1745322210134\x07POST git-upload-pack (175 bytes)",
		"\x1b_bk;t=1745322210213\x07remote: Counting objects:  50% (27/54)[K",
		"\x1b_bk;t=1745322210213\x07remote: Counting objects: 100% (54/54)[K",
		"\x1b_bk;t=1745322210213\x07remote: Counting objects: 100% (54/54), done.[K",
		"\x1b_bk;t=1745322210236\x07Receiving objects:  50% (131/263)",
		"\x1b_bk;t=1745322210340\x07Receiving objects: 100% (263/263), 607.65 KiB | 5.73 MiB/s, done.",
		"\x1b_bk;t=1745322210340\x07Resolving deltas:  50% (59/119)",
		"\x1b_bk;t=1745322210340\x07Resolving deltas: 100% (119/119), done.",
		"\x1b_bk;t=1745322210349\x07[90m$[0m git clean -ffxdq",
		"\x1b_bk;t=1745322210351\x07[90m$[0m git fetch -v --prune -- origin abc123",
		"\x1b_bk;t=1745322210692\x07~~~ Running script",
		"\x1b_bk;t=1745322210692\x07[90m$[0m ./script.sh",
		"\x1b_bk;t=1745322210694\x07--- :package: Build job checkout directory",
		"\x1b_bk;t=1745322210698\x07total 36",
		"\x1b_bk;t=1745322210698\x07drwxr-xr-x 5 root root 4096 Apr 22 11:43 .",
		"\x1b_bk;t=1745322210699\x07+++ :hammer: Example tests",
		"\x1b_bk;t=1745322210701\x07[33mCongratulations![0m You've successfully run your first build!",
		"Regular log line without timestamp",
		"Another regular line",
		"\x1b_bk;t=1745322210725\x07~~~ Uploading artifacts",
		"\x1b_bk;t=1745322210735\x07[38;5;48m2025-04-22 11:43:30 INFO[0m [0mFound 2 files[0m",
	}

	for i := 0; i < numLines; i++ {
		entry := entries[i%len(entries)]
		if i > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(entry)
	}

	return builder.String()
}

// BenchmarkParseLine tests the performance of parsing individual lines
func BenchmarkParseLine(b *testing.B) {
	parser := NewParser()
	line := "\x1b_bk;t=1745322209921\x07[90m$[0m /buildkite/agent/hooks/environment"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.ParseLine(line)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseLineNoTimestamp tests parsing lines without OSC sequences
func BenchmarkParseLineNoTimestamp(b *testing.B) {
	parser := NewParser()
	line := "Regular log line without timestamp information"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.ParseLine(line)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIterator tests the performance of the iterator approach
func BenchmarkIterator(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)
				iterator := parser.NewIterator(reader)

				count := 0
				for iterator.Next() {
					count++
				}

				if err := iterator.Err(); err != nil {
					b.Fatal(err)
				}

				if count != size {
					b.Fatalf("Expected %d entries, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkParseReaderLegacy tests the performance of collecting all entries in memory
func BenchmarkParseReaderLegacy(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)

				// Collect all entries using streaming iterator (equivalent to old ParseReader)
				var entries []*LogEntry
				for entry, err := range parser.All(reader) {
					if err != nil {
						b.Fatal(err)
					}
					entries = append(entries, entry)
				}

				if len(entries) != size {
					b.Fatalf("Expected %d entries, got %d", size, len(entries))
				}
			}
		})
	}
}

// BenchmarkIteratorWithFiltering tests iterator performance with filtering
func BenchmarkIteratorWithFiltering(b *testing.B) {
	data := generateTestData(10000)
	parser := NewParser()

	filters := []struct {
		name string
		fn   func(*LogEntry) bool
	}{
		{"commands", func(e *LogEntry) bool { return e.IsCommand() }},
		{"sections", func(e *LogEntry) bool { return e.IsSection() }},
		{"progress", func(e *LogEntry) bool { return e.IsProgress() }},
	}

	for _, filter := range filters {
		b.Run(filter.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)
				iterator := parser.NewIterator(reader)

				count := 0
				for iterator.Next() {
					if filter.fn(iterator.Entry()) {
						count++
					}
				}

				if err := iterator.Err(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkANSIStripping tests ANSI code removal performance
func BenchmarkANSIStripping(b *testing.B) {
	parser := NewParser()
	content := "[38;5;48m2025-04-22 11:43:30 INFO[0m [0mFound 2 files that match \"artifacts/*\"[0m"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = parser.StripANSI(content)
	}
}

// BenchmarkMemoryUsage provides a rough comparison of memory allocation patterns
func BenchmarkMemoryUsage(b *testing.B) {
	data := generateTestData(10000)
	parser := NewParser()

	b.Run("iterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)
			iterator := parser.NewIterator(reader)

			for iterator.Next() {
				// Just iterate, don't store
				_ = iterator.Entry()
			}

			if err := iterator.Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("collect_all_entries", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)

			// Collect all entries using streaming iterator
			var entries []*LogEntry
			for entry, err := range parser.All(reader) {
				if err != nil {
					b.Fatal(err)
				}
				entries = append(entries, entry)
			}

			// Access each entry to simulate usage
			for _, entry := range entries {
				_ = entry
			}
		}
	})
}

// BenchmarkSeq2Iterator tests the performance of the Go 1.23+ Seq2 iterator
func BenchmarkSeq2Iterator(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)

				count := 0
				for entry, err := range parser.All(reader) {
					if err != nil {
						b.Fatal(err)
					}
					count++
					_ = entry // Prevent optimization
				}

				if count != size {
					b.Fatalf("Expected %d entries, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkSeq2WithFiltering tests Seq2 iterator performance with filtering
func BenchmarkSeq2WithFiltering(b *testing.B) {
	data := generateTestData(10000)
	parser := NewParser()

	filters := []struct {
		name string
		fn   func(*LogEntry) bool
	}{
		{"commands", func(e *LogEntry) bool { return e.IsCommand() }},
		{"groups", func(e *LogEntry) bool { return e.IsGroup() }},
		{"progress", func(e *LogEntry) bool { return e.IsProgress() }},
	}

	for _, filter := range filters {
		b.Run(filter.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)

				count := 0
				for entry, err := range parser.All(reader) {
					if err != nil {
						b.Fatal(err)
					}
					if filter.fn(entry) {
						count++
					}
				}
			}
		})
	}
}

// BenchmarkIteratorComparison compares traditional iterator vs Seq2 performance
func BenchmarkIteratorComparison(b *testing.B) {
	data := generateTestData(10000)
	parser := NewParser()

	b.Run("traditional_iterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)
			iterator := parser.NewIterator(reader)

			count := 0
			for iterator.Next() {
				count++
				_ = iterator.Entry()
			}

			if err := iterator.Err(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("seq2_iterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)

			count := 0
			for entry, err := range parser.All(reader) {
				if err != nil {
					b.Fatal(err)
				}
				count++
				_ = entry
			}
		}
	})
}

// BenchmarkParquetExport tests Parquet export performance
func BenchmarkParquetExport(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			// Pre-parse entries to isolate export performance
			reader := strings.NewReader(data)
			var entries []*LogEntry
			for entry, err := range parser.All(reader) {
				if err != nil {
					b.Fatal(err)
				}
				entries = append(entries, entry)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filename := fmt.Sprintf("bench_test_%d_%d.parquet", size, i)

				err := ExportToParquet(entries, filename)
				if err != nil {
					b.Fatal(err)
				}

				// Cleanup
				_ = os.Remove(filename) // Ignore error in benchmark cleanup
			}
		})
	}
}

// BenchmarkParquetIteratorExport tests iterator-based Parquet export
func BenchmarkParquetIteratorExport(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)
				iterator := parser.NewIterator(reader)
				filename := fmt.Sprintf("bench_iter_%d_%d.parquet", size, i)

				err := ExportIteratorToParquet(iterator, filename)
				if err != nil {
					b.Fatal(err)
				}

				// Cleanup
				_ = os.Remove(filename) // Ignore error in benchmark cleanup
			}
		})
	}
}

// BenchmarkParquetSeq2Export tests Seq2-based Parquet export
func BenchmarkParquetSeq2Export(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("lines_%d", size), func(b *testing.B) {
			data := generateTestData(size)
			parser := NewParser()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)
				filename := fmt.Sprintf("bench_seq2_%d_%d.parquet", size, i)

				err := ExportSeq2ToParquet(parser.All(reader), filename)
				if err != nil {
					b.Fatal(err)
				}

				// Cleanup
				_ = os.Remove(filename) // Ignore error in benchmark cleanup
			}
		})
	}
}

// BenchmarkParquetExportComparison compares different Parquet export methods
func BenchmarkParquetExportComparison(b *testing.B) {
	data := generateTestData(1000)
	parser := NewParser()

	// Pre-parse for slice-based export
	reader := strings.NewReader(data)
	var entries []*LogEntry
	for entry, err := range parser.All(reader) {
		if err != nil {
			b.Fatal(err)
		}
		entries = append(entries, entry)
	}

	b.Run("slice_export", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filename := fmt.Sprintf("bench_slice_%d.parquet", i)

			err := ExportToParquet(entries, filename)
			if err != nil {
				b.Fatal(err)
			}

			_ = os.Remove(filename) // Ignore error in benchmark cleanup
		}
	})

	b.Run("iterator_export", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)
			iterator := parser.NewIterator(reader)
			filename := fmt.Sprintf("bench_iter_%d.parquet", i)

			err := ExportIteratorToParquet(iterator, filename)
			if err != nil {
				b.Fatal(err)
			}

			_ = os.Remove(filename) // Ignore error in benchmark cleanup
		}
	})

	b.Run("seq2_export", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)
			filename := fmt.Sprintf("bench_seq2_%d.parquet", i)

			err := ExportSeq2ToParquet(parser.All(reader), filename)
			if err != nil {
				b.Fatal(err)
			}

			_ = os.Remove(filename) // Ignore error in benchmark cleanup
		}
	})
}

// BenchmarkParquetWithFiltering tests filtered Parquet export performance
func BenchmarkParquetWithFiltering(b *testing.B) {
	data := generateTestData(5000)
	parser := NewParser()

	filterFunc := func(entry *LogEntry) bool {
		return entry.IsCommand() || entry.IsGroup()
	}

	b.Run("seq2_filtered", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(data)
			filename := fmt.Sprintf("bench_filtered_%d.parquet", i)

			err := ExportSeq2ToParquetWithFilter(parser.All(reader), filename, filterFunc)
			if err != nil {
				b.Fatal(err)
			}

			_ = os.Remove(filename) // Ignore error in benchmark cleanup
		}
	})
}

// BenchmarkByteParserCore tests the core byte parser performance
func BenchmarkByteParserCore(b *testing.B) {
	parser := NewByteParser()

	testCases := []struct {
		name string
		line string
	}{
		{"osc_with_timestamp", "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook"},
		{"regular_line", "regular log line without timestamp"},
		{"ansi_heavy", "\x1b_bk;t=1745322209921\x07\x1b[38;5;48m2025-04-22 11:43:30 INFO\x1b[0m \x1b[0mFound files\x1b[0m"},
		{"progress_line", "\x1b_bk;t=1745322210213\x07remote: Counting objects:  50% (27/54)[K"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := parser.ParseLine(tc.line)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkContentClassification tests entry classification performance
func BenchmarkContentClassification(b *testing.B) {
	data := generateTestData(1000)
	parser := NewParser()
	reader := strings.NewReader(data)

	// Pre-parse entries
	var entries []*LogEntry
	for entry, err := range parser.All(reader) {
		if err != nil {
			b.Fatal(err)
		}
		entries = append(entries, entry)
	}

	b.Run("is_command", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, entry := range entries {
				_ = entry.IsCommand()
			}
		}
	})

	b.Run("is_group", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, entry := range entries {
				_ = entry.IsGroup()
			}
		}
	})

	b.Run("is_progress", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, entry := range entries {
				_ = entry.IsProgress()
			}
		}
	})

	b.Run("clean_content", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, entry := range entries {
				_ = entry.CleanContent()
			}
		}
	})
}
