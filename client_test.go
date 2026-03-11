package buildkitelogs

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildkite/go-buildkite/v4"
)

// mockBuildkiteAPI implements BuildkiteAPI for testing with call tracking
type mockBuildkiteAPI struct {
	logContent     string
	jobStatus      *JobStatus
	getLogCalls    int
	getStatusCalls int
}

func (m *mockBuildkiteAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	m.getLogCalls++
	return io.NopCloser(strings.NewReader(m.logContent)), nil
}

func (m *mockBuildkiteAPI) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error) {
	m.getStatusCalls++
	return m.jobStatus, nil
}

func newTerminalMock() *mockBuildkiteAPI {
	return &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07Test log entry\n",
		jobStatus: &JobStatus{
			ID:         "test-job",
			State:      JobStatePassed,
			IsTerminal: true,
		},
	}
}

func newTestClient(t *testing.T, mock *mockBuildkiteAPI) *Client {
	t.Helper()
	tempDir := t.TempDir()
	client, err := NewClientWithAPI(context.Background(), mock, "file://"+tempDir)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestClient_NewClient(t *testing.T) {
	client, _ := buildkite.NewOpts()
	tempDir := t.TempDir()
	storageURL := "file://" + tempDir

	c, err := NewClient(context.Background(), client, storageURL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if c.storageURL != storageURL {
		t.Errorf("storageURL = %q, want %q", c.storageURL, storageURL)
	}
}

func TestClient_NewClientWithAPI(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)

	if client.api != mock {
		t.Error("api not set to mock")
	}
}

func TestClient_DownloadAndCache_Validation(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	tests := []struct {
		name                      string
		org, pipeline, build, job string
	}{
		{"missing org", "", "pipeline", "build", "job"},
		{"missing pipeline", "org", "", "build", "job"},
		{"missing build", "org", "pipeline", "", "job"},
		{"missing job", "org", "pipeline", "build", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.DownloadAndCache(ctx, tt.org, tt.pipeline, tt.build, tt.job, time.Minute, false)
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestClient_DownloadAndCache_HappyPath(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	path, err := client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("DownloadAndCache: %v", err)
	}

	if path == "" {
		t.Fatal("expected non-empty file path")
	}

	// Verify the file is a valid parquet file
	reader := NewParquetReader(ctx, path)
	info, err := reader.GetFileInfo()
	if err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}
	if info.RowCount == 0 {
		t.Error("expected non-zero row count")
	}
}

func TestClient_DownloadAndCache_CacheHit(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	// First call downloads
	_, err := client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first DownloadAndCache: %v", err)
	}

	initialLogCalls := mock.getLogCalls

	// Second call should use cache (terminal job)
	_, err = client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("second DownloadAndCache: %v", err)
	}

	if mock.getLogCalls != initialLogCalls {
		t.Errorf("expected no additional log downloads on cache hit, got %d extra calls", mock.getLogCalls-initialLogCalls)
	}
}

func TestClient_DownloadAndCache_ForceRefresh(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	// First call downloads
	_, err := client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first DownloadAndCache: %v", err)
	}

	initialLogCalls := mock.getLogCalls

	// Force refresh should re-download
	_, err = client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, true)
	if err != nil {
		t.Fatalf("force refresh DownloadAndCache: %v", err)
	}

	if mock.getLogCalls == initialLogCalls {
		t.Error("expected additional log download on force refresh")
	}
}

func TestClient_DownloadAndCache_Hooks(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	var (
		cacheCheckCalled  bool
		jobStatusCalled   bool
		logDownloadCalled bool
		logParsingCalled  bool
		blobStorageCalled bool
		localCacheCalled  bool
	)

	client.Hooks().AddAfterCacheCheck(func(ctx context.Context, r *CacheCheckResult) {
		cacheCheckCalled = true
		if r.Org != "org" {
			t.Errorf("CacheCheck hook: org = %q, want %q", r.Org, "org")
		}
	})
	client.Hooks().AddAfterJobStatus(func(ctx context.Context, r *JobStatusResult) {
		jobStatusCalled = true
		if !r.JobStatus.IsTerminal {
			t.Error("JobStatus hook: expected terminal job")
		}
	})
	client.Hooks().AddAfterLogDownload(func(ctx context.Context, r *LogDownloadResult) {
		logDownloadCalled = true
	})
	client.Hooks().AddAfterLogParsing(func(ctx context.Context, r *LogParsingResult) {
		logParsingCalled = true
		if r.ParquetSize == 0 {
			t.Error("LogParsing hook: expected non-zero parquet size")
		}
	})
	client.Hooks().AddAfterBlobStorage(func(ctx context.Context, r *BlobStorageResult) {
		blobStorageCalled = true
		if !r.IsTerminal {
			t.Error("BlobStorage hook: expected terminal")
		}
	})
	client.Hooks().AddAfterLocalCache(func(ctx context.Context, r *LocalCacheResult) {
		localCacheCalled = true
		if r.LocalPath == "" {
			t.Error("LocalCache hook: expected non-empty path")
		}
	})

	_, err := client.DownloadAndCache(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("DownloadAndCache: %v", err)
	}

	if !cacheCheckCalled {
		t.Error("AfterCacheCheck hook not called")
	}
	if !jobStatusCalled {
		t.Error("AfterJobStatus hook not called")
	}
	if !logDownloadCalled {
		t.Error("AfterLogDownload hook not called")
	}
	if !logParsingCalled {
		t.Error("AfterLogParsing hook not called")
	}
	if !blobStorageCalled {
		t.Error("AfterBlobStorage hook not called")
	}
	if !localCacheCalled {
		t.Error("AfterLocalCache hook not called")
	}
}

func TestClient_NewReader(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := context.Background()

	reader, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Verify we can read entries from the reader
	count := 0
	for _, err := range reader.ReadEntriesIter() {
		if err != nil {
			t.Fatalf("ReadEntriesIter: %v", err)
		}
		count++
	}

	if count == 0 {
		t.Error("expected entries from reader")
	}
}

func TestClient_Close(t *testing.T) {
	mock := newTerminalMock()
	tempDir := t.TempDir()
	client, err := NewClientWithAPI(context.Background(), mock, "file://"+tempDir)
	if err != nil {
		t.Fatalf("NewClientWithAPI: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
