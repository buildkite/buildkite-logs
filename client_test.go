package buildkitelogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildkite/buildkite-logs/logparser"
	"github.com/buildkite/go-buildkite/v5"
)

// mockBuildkiteAPI implements BuildkiteAPI for testing with call tracking
type mockBuildkiteAPI struct {
	logContent     string
	jobStatus      *JobStatus
	getLogCalls    int
	getStatusCalls int
	logDelay       time.Duration
	logStarted     chan struct{}
	statusErr      error
	logExistsErr   error
	logMissing     bool
	getExistsCalls int
	logErr         error
	mu             sync.Mutex
}

func (m *mockBuildkiteAPI) JobLogExists(ctx context.Context, org, pipeline, build, job string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getExistsCalls++
	if m.logExistsErr != nil {
		return false, m.logExistsErr
	}
	return !m.logMissing, nil
}

func (m *mockBuildkiteAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	if m.logStarted != nil {
		select {
		case m.logStarted <- struct{}{}:
		default:
		}
	}
	if m.logDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.logDelay):
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getLogCalls++
	if m.logErr != nil {
		return nil, m.logErr
	}
	return io.NopCloser(strings.NewReader(m.logContent)), nil
}

func (m *mockBuildkiteAPI) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getStatusCalls++
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	return m.jobStatus, nil
}

func (m *mockBuildkiteAPI) calls() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getLogCalls, m.getStatusCalls
}

func (m *mockBuildkiteAPI) existsCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getExistsCalls
}

func (m *mockBuildkiteAPI) setJobStatus(status *JobStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobStatus = status
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

func newTestClient(t *testing.T, mock *mockBuildkiteAPI, opts ...ClientOption) *Client {
	t.Helper()
	tempDir := t.TempDir()
	client, err := NewClientWithAPI(t.Context(), mock, "file://"+tempDir, opts...)
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

	c, err := NewClient(t.Context(), client, storageURL, WithParserOptions(
		logparser.WithMaxLineBytes(12),
		logparser.WithTruncationSuffix("[cut]"),
	))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if c.storageURL != storageURL {
		t.Errorf("storageURL = %q, want %q", c.storageURL, storageURL)
	}
	entries, err := parseWithClientParser(c, "0123456789abcdef\n")
	if err != nil {
		t.Fatalf("client parser error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("client parser entries = %d, want 1", len(entries))
	}
	if got := entries[0].Content; got != "0123456[cut]" {
		t.Fatalf("client parser content = %q, want %q", got, "0123456[cut]")
	}
}

func TestClient_NewClientWithAPI(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock, WithParserOptions(
		logparser.WithMaxLineBytes(12),
		logparser.WithTruncateLongLines(false),
	))

	if client.api != mock {
		t.Error("api not set to mock")
	}
	if _, err := parseWithClientParser(client, "0123456789abcdef\n"); err == nil {
		t.Fatal("expected client parser to reject long lines when truncation is disabled")
	}
}

func TestClient_WithParserOptionsAcceptsNilOption(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock, WithParserOptions(nil))

	if _, err := parseWithClientParser(client, "0123456789abcdef\n"); err != nil {
		t.Fatalf("nil parser option should keep a usable parser: %v", err)
	}
}

func parseWithClientParser(client *Client, input string) ([]*logparser.Entry, error) {
	parser := client.newDefaultClientParser()
	var entries []*logparser.Entry
	for entry, err := range parser.All(strings.NewReader(input)) {
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func TestClient_NewReader_Validation(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

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
			_, err := client.NewReader(ctx, tt.org, tt.pipeline, tt.build, tt.job, time.Minute, false)
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestClient_NewReader_HappyPath(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	reader, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	// Verify the file is a valid parquet file
	info, err := reader.GetFileInfo()
	if err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}
	if info.RowCount == 0 {
		t.Error("expected non-zero row count")
	}
}

func TestClient_NewReader_CacheHit(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	// First call downloads
	reader1, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first NewReader: %v", err)
	}
	defer reader1.Close()

	initialLogCalls := mock.getLogCalls

	// The terminal cache remains usable, but current log access is checked.
	reader2, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("second NewReader: %v", err)
	}
	defer reader2.Close()

	if mock.getLogCalls != initialLogCalls {
		t.Errorf("expected no additional log downloads on cache hit, got %d extra calls", mock.getLogCalls-initialLogCalls)
	}
	if _, statusCalls := mock.calls(); statusCalls != 1 {
		t.Errorf("GetJobStatus calls = %d, want 1", statusCalls)
	}
	if existsCalls := mock.existsCalls(); existsCalls != 1 {
		t.Errorf("JobLogExists calls = %d, want 1", existsCalls)
	}
}

func TestClient_NewReader_RefreshesBlobWithoutMetadata(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	blobKey := GenerateBlobKey("org", "pipeline", "123", "job-1")

	if err := client.blobStorage.WriteWithMetadata(t.Context(), blobKey, []byte("legacy cache data"), nil); err != nil {
		t.Fatalf("WriteWithMetadata: %v", err)
	}
	metadata, err := client.blobStorage.ReadWithMetadata(t.Context(), blobKey)
	if err != nil {
		t.Fatalf("ReadWithMetadata before refresh: %v", err)
	}
	if metadata != nil {
		t.Fatalf("metadata before refresh = %#v, want nil", metadata)
	}

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.GetFileInfo(); err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}

	logCalls, statusCalls := mock.calls()
	if logCalls != 1 {
		t.Fatalf("GetJobLog calls = %d, want 1", logCalls)
	}
	if statusCalls != 1 {
		t.Fatalf("GetJobStatus calls = %d, want 1", statusCalls)
	}
	metadata, err = client.blobStorage.ReadWithMetadata(t.Context(), blobKey)
	if err != nil {
		t.Fatalf("ReadWithMetadata after refresh: %v", err)
	}
	if metadata == nil {
		t.Fatal("metadata after refresh = nil")
	}
}

func TestClient_TerminalCacheHit_RequiresJobAccess(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("initial NewReader: %v", err)
	}
	defer reader.Close()

	mock.logExistsErr = errors.New("job access denied")

	_, err = client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if !errors.Is(err, mock.logExistsErr) {
		t.Fatalf("NewReader error = %v, want %v", err, mock.logExistsErr)
	}

	logCalls, _ := mock.calls()
	if logCalls != 1 {
		t.Fatalf("GetJobLog calls = %d, want 1", logCalls)
	}

	mock.logExistsErr = nil
	mock.logMissing = true
	_, err = client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if !errors.Is(err, ErrJobLogUnavailable) {
		t.Fatalf("missing log error = %v, want %v", err, ErrJobLogUnavailable)
	}
}

func TestClient_SharedCacheHit_UsesCurrentClientIdentity(t *testing.T) {
	storageURL := "file://" + t.TempDir()
	authorizedAPI := newTerminalMock()
	authorizedClient, err := NewClientWithAPI(t.Context(), authorizedAPI, storageURL)
	if err != nil {
		t.Fatalf("NewClientWithAPI authorized: %v", err)
	}
	defer authorizedClient.Close()

	reader, err := authorizedClient.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("populate shared cache: %v", err)
	}
	reader.Close()

	accessDenied := errors.New("job log access denied")
	unauthorizedAPI := newTerminalMock()
	unauthorizedAPI.logExistsErr = accessDenied
	unauthorizedClient, err := NewClientWithAPI(t.Context(), unauthorizedAPI, storageURL)
	if err != nil {
		t.Fatalf("NewClientWithAPI unauthorized: %v", err)
	}
	defer unauthorizedClient.Close()

	_, err = unauthorizedClient.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if !errors.Is(err, accessDenied) {
		t.Fatalf("shared cache read error = %v, want %v", err, accessDenied)
	}
	logCalls, statusCalls := unauthorizedAPI.calls()
	if logCalls != 0 || statusCalls != 0 {
		t.Fatalf("unauthorized API calls: log=%d status=%d, want 0 for both", logCalls, statusCalls)
	}
	if existsCalls := unauthorizedAPI.existsCalls(); existsCalls != 1 {
		t.Fatalf("JobLogExists calls = %d, want 1", existsCalls)
	}
}

func TestClient_ConcurrentCacheMiss_SingleFlight(t *testing.T) {
	mock := newTerminalMock()
	mock.logDelay = 25 * time.Millisecond
	client := newTestClient(t, mock)

	const goroutines = 8
	runConcurrentReaders(t, client, goroutines, false, time.Minute)

	logCalls, statusCalls := mock.calls()
	if logCalls != 1 {
		t.Fatalf("GetJobLog calls = %d, want 1", logCalls)
	}
	if statusCalls != 1 {
		t.Fatalf("GetJobStatus calls = %d, want 1", statusCalls)
	}
}

func TestClient_ConcurrentTTLExpiry_RefreshOnce(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07running log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStateRunning, IsTerminal: false},
		logDelay:   25 * time.Millisecond,
	}
	client := newTestClient(t, mock)

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Nanosecond, false)
	if err != nil {
		t.Fatalf("initial NewReader: %v", err)
	}
	defer reader.Close()
	time.Sleep(time.Millisecond)

	runConcurrentReaders(t, client, 8, false, time.Nanosecond)

	logCalls, statusCalls := mock.calls()
	if logCalls != 2 {
		t.Fatalf("GetJobLog calls = %d, want 2", logCalls)
	}
	if statusCalls != 2 {
		t.Fatalf("GetJobStatus calls = %d, want 2", statusCalls)
	}
	if existsCalls := mock.existsCalls(); existsCalls != 8 {
		t.Fatalf("JobLogExists calls = %d, want 8", existsCalls)
	}
}

func TestClient_NonTerminalCacheHit_ChecksJobLogAccess(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07running log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStateRunning, IsTerminal: false},
	}
	client := newTestClient(t, mock)

	reader1, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first NewReader: %v", err)
	}
	defer reader1.Close()

	reader2, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("second NewReader: %v", err)
	}
	defer reader2.Close()

	logCalls, statusCalls := mock.calls()
	if logCalls != 1 {
		t.Fatalf("GetJobLog calls = %d, want 1", logCalls)
	}
	if statusCalls != 2 {
		t.Fatalf("GetJobStatus calls = %d, want 2", statusCalls)
	}
	if existsCalls := mock.existsCalls(); existsCalls != 1 {
		t.Fatalf("JobLogExists calls = %d, want 1", existsCalls)
	}
}

func TestClient_NonTerminalCacheHit_RefreshesWhenStatusBecomesTerminal(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07running log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStateRunning, IsTerminal: false},
	}
	client := newTestClient(t, mock)
	blobKey := GenerateBlobKey("org", "pipeline", "123", "job-1")

	reader1, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first NewReader: %v", err)
	}
	defer reader1.Close()
	mock.setJobStatus(&JobStatus{ID: "test-job", State: JobStatePassed, IsTerminal: true})

	reader2, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("second NewReader: %v", err)
	}
	defer reader2.Close()

	logCalls, statusCalls := mock.calls()
	if logCalls != 2 {
		t.Fatalf("GetJobLog calls = %d, want 2", logCalls)
	}
	if statusCalls != 2 {
		t.Fatalf("GetJobStatus calls = %d, want 2", statusCalls)
	}

	metadata, err := client.blobStorage.ReadWithMetadata(t.Context(), blobKey)
	if err != nil {
		t.Fatalf("ReadWithMetadata: %v", err)
	}
	if !metadata.IsTerminal {
		t.Fatal("expected final refresh to persist terminal metadata")
	}
}

func TestClient_NewReader_RecordsLogSizeWithoutLimit(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock, WithMaxLogBytes(0))

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	metadata, err := client.blobStorage.ReadWithMetadata(t.Context(), GenerateBlobKey("org", "pipeline", "123", "job-1"))
	if err != nil {
		t.Fatalf("ReadWithMetadata: %v", err)
	}
	if metadata.LogSize != int64(len(mock.logContent)) {
		t.Fatalf("LogSize = %d, want %d", metadata.LogSize, len(mock.logContent))
	}
}

func TestClient_ConcurrentForceRefresh_Coalesces(t *testing.T) {
	mock := newTerminalMock()
	mock.logDelay = 25 * time.Millisecond
	client := newTestClient(t, mock)

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("initial NewReader: %v", err)
	}
	defer reader.Close()

	runConcurrentReaders(t, client, 8, true, time.Minute)

	logCalls, statusCalls := mock.calls()
	if logCalls != 2 {
		t.Fatalf("GetJobLog calls = %d, want 2", logCalls)
	}
	if statusCalls != 2 {
		t.Fatalf("GetJobStatus calls = %d, want 2", statusCalls)
	}
}

func TestClient_ConcurrentForceAndTTLRefresh_Coalesce(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07running log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStateRunning, IsTerminal: false},
		logDelay:   25 * time.Millisecond,
	}
	client := newTestClient(t, mock)

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Nanosecond, false)
	if err != nil {
		t.Fatalf("initial NewReader: %v", err)
	}
	defer reader.Close()
	time.Sleep(time.Millisecond)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, forceRefresh := range []bool{false, true} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Nanosecond, forceRefresh)
			if err != nil {
				errs <- err
				return
			}
			defer reader.Close()
			if _, err := reader.GetFileInfo(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent NewReader failed: %v", err)
	}

	logCalls, statusCalls := mock.calls()
	if logCalls != 2 {
		t.Fatalf("GetJobLog calls = %d, want 2", logCalls)
	}
	if statusCalls != 2 {
		t.Fatalf("GetJobStatus calls = %d, want 2", statusCalls)
	}
}

func TestClient_RefreshContinuesWhenSingleflightLeaderCancels(t *testing.T) {
	mock := newTerminalMock()
	mock.logDelay = 50 * time.Millisecond
	mock.logStarted = make(chan struct{}, 1)
	client := newTestClient(t, mock)

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderErr := make(chan error, 1)
	go func() {
		reader, err := client.NewReader(leaderCtx, "org", "pipeline", "123", "job-1", time.Minute, false)
		if reader != nil {
			_ = reader.Close()
		}
		leaderErr <- err
	}()

	<-mock.logStarted
	cancelLeader()

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("follower NewReader: %v", err)
	}
	defer reader.Close()

	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}

	logCalls, statusCalls := mock.calls()
	if logCalls != 1 {
		t.Fatalf("GetJobLog calls = %d, want 1", logCalls)
	}
	if statusCalls != 1 {
		t.Fatalf("GetJobStatus calls = %d, want 1", statusCalls)
	}
}

func TestClient_NewReader_JobStatusErrorHook(t *testing.T) {
	statusErr := errors.New("status unavailable")
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07Test log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStateRunning, IsTerminal: false},
		statusErr:  statusErr,
	}
	client := newTestClient(t, mock)

	var hookResult *JobStatusResult
	client.Hooks().AddAfterJobStatus(func(ctx context.Context, r *JobStatusResult) {
		hookResult = r
	})

	_, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err == nil {
		t.Fatal("expected NewReader to fail")
	}
	if hookResult == nil {
		t.Fatal("expected job status hook to fire")
	}
	if hookResult.Success {
		t.Fatal("expected hook result to report failure")
	}
	if !errors.Is(hookResult.Err, statusErr) {
		t.Fatalf("hook error = %v, want %v", hookResult.Err, statusErr)
	}
	if hookResult.Stage != StageJobStatus {
		t.Fatalf("hook stage = %q, want %q", hookResult.Stage, StageJobStatus)
	}
}

func TestClient_NewReader_CustomAPIReturnsNilJobStatus(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07Test log entry\n",
	}
	client := newTestClient(t, mock)

	var statusResult *JobStatusResult
	client.Hooks().AddAfterJobStatus(func(ctx context.Context, result *JobStatusResult) {
		statusResult = result
	})

	_, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err == nil {
		t.Fatal("NewReader error = nil, want nil job status error")
	}
	if !strings.Contains(err.Error(), "API returned nil job status") {
		t.Fatalf("NewReader error = %q, want nil job status error", err)
	}
	logCalls, statusCalls := mock.calls()
	if logCalls != 0 {
		t.Fatalf("GetJobLog calls = %d, want 0", logCalls)
	}
	if statusCalls != 1 {
		t.Fatalf("GetJobStatus calls = %d, want 1", statusCalls)
	}
	if statusResult == nil {
		t.Fatal("expected job status hook to fire")
	}
	if statusResult.Success {
		t.Fatal("job status hook reported success for nil status")
	}
	if statusResult.Err == nil || !strings.Contains(statusResult.Err.Error(), "API returned nil job status") {
		t.Fatalf("job status hook error = %v, want nil job status error", statusResult.Err)
	}
}

func TestClient_NewReader_LogDownloadStreamErrorHook(t *testing.T) {
	downloadErr := errors.New("buildkite returned 500")
	mock := &mockBuildkiteAPI{
		logContent: "\x1b_bk;t=1745322209921\x07Test log entry\n",
		jobStatus:  &JobStatus{ID: "test-job", State: JobStatePassed, IsTerminal: true},
	}
	client := newTestClient(t, mock)
	mock.logErr = nil

	var downloadResult *LogDownloadResult
	var parsingCalled bool
	client.Hooks().AddAfterLogDownload(func(ctx context.Context, r *LogDownloadResult) {
		downloadResult = r
	})
	client.Hooks().AddAfterLogParsing(func(ctx context.Context, r *LogParsingResult) {
		parsingCalled = true
	})

	mockWithStreamErr := &streamErrorBuildkiteAPI{
		status: mock.jobStatus,
		err:    &logDownloadError{err: downloadErr},
	}
	client.api = mockWithStreamErr

	_, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err == nil {
		t.Fatal("expected NewReader to fail")
	}
	if downloadResult == nil {
		t.Fatal("expected log download hook to fire")
	}
	if downloadResult.Success {
		t.Fatal("expected log download hook to report failure")
	}
	if !errors.Is(downloadResult.Err, downloadErr) {
		t.Fatalf("download hook error = %v, want %v", downloadResult.Err, downloadErr)
	}
	if parsingCalled {
		t.Fatal("log parsing hook should not fire for stream download errors")
	}
}

type streamErrorBuildkiteAPI struct {
	status *JobStatus
	err    error
}

func (a *streamErrorBuildkiteAPI) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error) {
	return a.status, nil
}

func (a *streamErrorBuildkiteAPI) JobLogExists(ctx context.Context, org, pipeline, build, job string) (bool, error) {
	return true, nil
}

func (a *streamErrorBuildkiteAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	return &errorReadCloser{err: a.err}, nil
}

type errorReadCloser struct {
	err error
}

func (r *errorReadCloser) Read(p []byte) (int, error) {
	return 0, r.err
}

func (r *errorReadCloser) Close() error {
	return nil
}

func runConcurrentReaders(t *testing.T, client *Client, goroutines int, forceRefresh bool, ttl time.Duration) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", ttl, forceRefresh)
			if err != nil {
				errs <- err
				return
			}
			defer reader.Close()
			if _, err := reader.GetFileInfo(); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent NewReader failed: %v", err)
	}
}

func TestClient_NewReader_ForceRefresh(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	// First call downloads
	reader1, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("first NewReader: %v", err)
	}
	defer reader1.Close()

	initialLogCalls := mock.getLogCalls

	// Force refresh should re-download
	reader2, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, true)
	if err != nil {
		t.Fatalf("force refresh NewReader: %v", err)
	}
	defer reader2.Close()

	if mock.getLogCalls == initialLogCalls {
		t.Error("expected additional log download on force refresh")
	}
}

func TestClient_NewReader_Hooks(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

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

	reader, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

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

func TestClient_NewReader_ReadEntries(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)
	ctx := t.Context()

	reader, err := client.NewReader(ctx, "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()

	// Verify we can read entries from the reader
	count := 0
	for _, err := range reader.ReadEntriesIter(ctx) {
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
	client, err := NewClientWithAPI(t.Context(), mock, "file://"+tempDir)
	if err != nil {
		t.Fatalf("NewClientWithAPI: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestClient_DefaultMaxLogBytes(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock)

	if client.maxLogBytes != DefaultMaxLogBytes {
		t.Errorf("maxLogBytes = %d, want %d", client.maxLogBytes, DefaultMaxLogBytes)
	}
}

func TestClient_WithMaxLogBytes(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock, WithMaxLogBytes(1024))

	if client.maxLogBytes != 1024 {
		t.Errorf("maxLogBytes = %d, want 1024", client.maxLogBytes)
	}
}

func TestClient_WithMaxLogBytes_Zero_DisablesLimit(t *testing.T) {
	mock := newTerminalMock()
	client := newTestClient(t, mock, WithMaxLogBytes(0))

	if client.maxLogBytes != 0 {
		t.Errorf("maxLogBytes = %d, want 0", client.maxLogBytes)
	}

	// Should succeed with no limit
	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader with no limit: %v", err)
	}
	defer reader.Close()
}

func TestClient_NewReader_LogTooLarge(t *testing.T) {
	mock := &mockBuildkiteAPI{
		logContent: strings.Repeat("x", 1024), // 1KB log
		jobStatus: &JobStatus{
			ID:         "test-job",
			State:      JobStatePassed,
			IsTerminal: true,
		},
	}
	client := newTestClient(t, mock, WithMaxLogBytes(100)) // 100 byte limit

	_, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err == nil {
		t.Fatal("expected ErrLogTooLarge, got nil")
	}
	if !errors.Is(err, ErrLogTooLarge) {
		t.Errorf("expected ErrLogTooLarge, got: %v", err)
	}
}

func TestClient_NewReader_LogWithinLimit(t *testing.T) {
	mock := newTerminalMock()                                    // small log content
	client := newTestClient(t, mock, WithMaxLogBytes(1024*1024)) // 1MB limit

	reader, err := client.NewReader(t.Context(), "org", "pipeline", "123", "job-1", time.Minute, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer reader.Close()
}

// mockRetriedJobAPI simulates the Buildkite API behavior when a job has been retried.
// The original job ID is replaced in the build's jobs array, but the replacement job
// references it via RetrySource. The log endpoint still works for both job IDs.
// See: https://github.com/buildkite/buildkite-mcp-server/issues/228
type mockRetriedJobAPI struct {
	originalJobID    string
	replacementJobID string
	logContent       string
	getLogCalls      int
	getStatusCalls   int
}

func (m *mockRetriedJobAPI) JobLogExists(ctx context.Context, org, pipeline, build, job string) (bool, error) {
	return true, nil
}

func (m *mockRetriedJobAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	m.getLogCalls++
	// The raw log endpoint works for both original and replacement job IDs
	return io.NopCloser(strings.NewReader(m.logContent)), nil
}

func (m *mockRetriedJobAPI) GetJobStatus(ctx context.Context, org, pipeline, build, jobID string) (*JobStatus, error) {
	m.getStatusCalls++
	// Simulate the real API: the original job is no longer in the build's jobs array
	if jobID == m.originalJobID {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	// The replacement job is found normally
	if jobID == m.replacementJobID {
		return &JobStatus{
			ID:         m.replacementJobID,
			State:      JobStateFailed,
			IsTerminal: true,
		}, nil
	}
	return nil, fmt.Errorf("job not found: %s", jobID)
}

func TestClient_NewReader_RetriedJob_OriginalJobNotFound(t *testing.T) {
	// This test verifies the bug from buildkite-mcp-server#228 at the Client level:
	// When GetJobStatus returns "job not found" for a retried job, NewReader fails.
	// This is the pre-fix behavior that the GetJobStatus fix resolves.
	mock := &mockRetriedJobAPI{
		originalJobID:    "019ce437-f44f-4528-9d74-dfae306fed69",
		replacementJobID: "019ce438-3423-432e-a4f2-b0bfbffcc980",
		logContent:       "\x1b_bk;t=1745322209921\x07Original job log output\n",
	}

	tempDir := t.TempDir()
	client, err := NewClientWithAPI(t.Context(), mock, "file://"+tempDir)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// The mock still returns "job not found" (simulating unfixed GetJobStatus),
	// so NewReader fails for the original job ID.
	_, err = client.NewReader(t.Context(), "bk-mark-wolfe", "starter-pipeline", "76", mock.originalJobID, time.Minute, false)
	if err == nil {
		t.Fatal("expected error for retried job, got nil")
	}

	if !strings.Contains(err.Error(), "job not found") {
		t.Errorf("expected 'job not found' error, got: %v", err)
	}

	// The replacement job ID works fine
	reader, err := client.NewReader(t.Context(), "bk-mark-wolfe", "starter-pipeline", "76", mock.replacementJobID, time.Minute, false)
	if err != nil {
		t.Fatalf("expected replacement job to work, got: %v", err)
	}
	defer reader.Close()
}
