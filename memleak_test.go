package buildkitelogs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestParquetWriter_ArrowMemoryReleased verifies that all Arrow-allocated memory is
// freed after a full write+close cycle. Uses memory.NewCheckedAllocator which
// tracks every allocation and panics if any are still outstanding when checked.
func TestParquetWriter_ArrowMemoryReleased(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)

	file, err := os.CreateTemp(t.TempDir(), "*.parquet")
	if err != nil {
		t.Fatal(err)
	}

	pw, err := newParquetWriterWithPool(file, checked)
	if err != nil {
		t.Fatal(err)
	}

	entries := makeTestEntries(100)
	if err := pw.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}
	// Write a second batch to exercise the reused builders.
	if err := pw.WriteBatch(makeTestEntries(50)); err != nil {
		t.Fatal(err)
	}

	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	// AssertSize panics (reported as test failure) if any Arrow buffers are
	// still allocated — i.e. a Release() call was missed somewhere.
	checked.AssertSize(t, 0)
}

// TestParquetWriter_ArrowMemoryReleased_MultipleWriters runs multiple
// ParquetWriter instances sequentially to catch leaks that only appear
// after the first object is reclaimed.
func TestParquetWriter_ArrowMemoryReleased_MultipleWriters(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)

	for i := range 5 {
		file, err := os.CreateTemp(t.TempDir(), "*.parquet")
		if err != nil {
			t.Fatal(err)
		}

		pw, err := newParquetWriterWithPool(file, checked)
		if err != nil {
			t.Fatal(err)
		}

		if err := pw.WriteBatch(makeTestEntries(200)); err != nil {
			t.Fatalf("iteration %d WriteBatch: %v", i, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("iteration %d Close: %v", i, err)
		}
	}

	checked.AssertSize(t, 0)
}

// TestClient_HeapStable checks that repeated DownloadAndCache calls do not cause
// unbounded Go heap growth. It forces a GC before and after N iterations and
// compares HeapInuse. A large difference (>2x) indicates a leak.
//
// This test is intentionally loose — GC timing and allocator internals mean
// the heap will not be exactly the same — but a true leak will show clear growth.
func TestClient_HeapStable(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	const iterations = 20

	// Warm up: run a few iterations before measuring so one-time init costs
	// don't skew the baseline.
	for range 3 {
		path, err := client.downloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, true)
		if err != nil {
			t.Fatal(err)
		}
		os.Remove(path)
	}

	runtime.GC()
	runtime.GC() // two passes to collect finalizer-dependent objects

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := range iterations {
		path, err := client.downloadAndCache(ctx, "org", "pipeline", fmt.Sprintf("%d", i), "job-1", time.Minute, true)
		if err != nil {
			t.Fatal(err)
		}
		os.Remove(path) // caller is responsible for cleaning up the returned temp file
	}

	runtime.GC()
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse in bytes — allow up to 4× growth as a generous threshold.
	// A true leak grows without bound; legitimate post-GC variance is small.
	if after.HeapInuse > before.HeapInuse*4+4*1024*1024 {
		t.Errorf("heap appears to be growing: before=%d bytes, after=%d bytes (%d iterations)",
			before.HeapInuse, after.HeapInuse, iterations)
	}
}

// TestClient_NoTempFileAccumulation verifies that each DownloadAndCache call does
// not leave behind unreturned bklog-* temp files beyond the one path it returns.
//
// The DownloadAndCache implementation creates an intermediate bklog-*.parquet temp
// file (cleaned up internally via defer) and a final bklog- temp file (returned to
// the caller). This test verifies the internal temp file is always removed.
func TestClient_NoTempFileAccumulation(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	const iterations = 10

	// Snapshot bklog-* files that already exist before this test touches anything.
	before := snapshotBklogFiles(t)

	returnedPaths := make(map[string]bool, iterations)
	for i := range iterations {
		path, err := client.downloadAndCache(ctx, "org", "pipeline", fmt.Sprintf("%d", i), "job-1", time.Minute, true)
		if err != nil {
			t.Fatal(err)
		}
		returnedPaths[path] = true
		t.Cleanup(func() { os.Remove(path) })
	}

	// Any bklog-* file that appeared after our calls but is NOT a returned path
	// is an internal temp file that leaked.
	after := snapshotBklogFiles(t)
	var leaked []string
	for path := range after {
		if !before[path] && !returnedPaths[path] {
			leaked = append(leaked, path)
		}
	}

	if len(leaked) > 0 {
		t.Errorf("found %d internal bklog-* temp files not cleaned up after %d DownloadAndCache calls:\n%s",
			len(leaked), iterations, strings.Join(leaked, "\n"))
	}
}

// snapshotBklogFiles returns the set of bklog-* paths currently in os.TempDir().
func snapshotBklogFiles(t *testing.T) map[string]bool {
	t.Helper()
	result := map[string]bool{}
	_ = filepath.WalkDir(os.TempDir(), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), "bklog-") {
			result[path] = true
		}
		return nil
	})
	return result
}

func makeTestEntries(n int) []*LogEntry {
	base := time.Date(2025, 4, 22, 12, 0, 0, 0, time.UTC)
	entries := make([]*LogEntry, n)
	for i := range entries {
		entries[i] = &LogEntry{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
			Content:   fmt.Sprintf("log line %d", i),
			Group:     "test-group",
		}
	}
	return entries
}
