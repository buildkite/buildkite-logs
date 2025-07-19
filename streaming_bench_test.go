package buildkitelogs

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRealWorldStreamingPerformance tests streaming performance with actual parquet files
func TestRealWorldStreamingPerformance(t *testing.T) {
	testFiles := []struct {
		name string
		path string
	}{
		{"small", "testdata/bash-example.parquet"},
		{"medium", "testdata/bazel-bazel_build_32517_rocky-rocky-linux-8.parquet"},
		{"large", "testdata/bun_build_19487_windows-x64-build-cpp.parquet"},
	}

	for _, tf := range testFiles {
		t.Run(tf.name, func(t *testing.T) {
			if _, err := os.Stat(tf.path); os.IsNotExist(err) {
				t.Skipf("Test file %s not found", tf.path)
			}

			reader := NewParquetReader(tf.path)

			// Test streaming iterator performance
			start := time.Now()
			iterCount := 0
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					t.Fatalf("ReadEntriesIter failed: %v", err)
				}
				iterCount++
				_ = entry
			}
			iterTime := time.Since(start)

			// Test early termination performance
			start = time.Now()
			earlyCount := 0
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					t.Fatalf("ReadEntriesIter failed: %v", err)
				}
				earlyCount++
				_ = entry
				if earlyCount >= 1000 {
					break
				}
			}
			earlyTime := time.Since(start)

			// Report results
			t.Logf("File: %s", tf.name)
			t.Logf("Full streaming: %d entries in %v", iterCount, iterTime)
			t.Logf("Early termination: %d entries in %v", earlyCount, earlyTime)
			if iterCount > 1000 {
				t.Logf("Early termination speedup: %.2fx", float64(iterTime)/float64(earlyTime))
			}
		})
	}
}

// BenchmarkStreamingFiles benchmarks streaming with actual parquet files
func BenchmarkStreamingFiles(b *testing.B) {
	testFiles := []struct {
		name string
		path string
	}{
		{"large", "testdata/bun_build_19487_windows-x64-build-cpp.parquet"},
	}

	for _, tf := range testFiles {
		if _, err := os.Stat(tf.path); os.IsNotExist(err) {
			b.Skipf("Test file %s not found", tf.path)
		}

		reader := NewParquetReader(tf.path)

		b.Run(fmt.Sprintf("%s_StreamingFull", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
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

		b.Run(fmt.Sprintf("%s_StreamingGroupAnalysis", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
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
				_ = groupMap
			}
		})

		b.Run(fmt.Sprintf("%s_EarlyTermination", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				count := 0
				for entry, err := range reader.ReadEntriesIter() {
					if err != nil {
						b.Fatalf("ReadEntriesIter failed: %v", err)
					}

					_ = entry.Content
					count++
					if count >= 1000 {
						break // Early termination advantage
					}
				}
			}
		})

		b.Run(fmt.Sprintf("%s_FilteredStreaming", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				count := 0
				for entry, err := range reader.FilterByGroupIter("test") {
					if err != nil {
						b.Fatalf("FilterByGroupIter failed: %v", err)
					}
					count++
					_ = entry
					if count >= 100 {
						break // Early termination
					}
				}
			}
		})
	}
}
