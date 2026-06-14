package buildkitelogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/buildkite/go-buildkite/v5"
	"golang.org/x/sync/singleflight"
)

// ErrLogTooLarge is returned when a job log exceeds the configured maximum size.
var ErrLogTooLarge = errors.New("log exceeds maximum allowed size")

// DefaultMaxLogBytes is the default maximum log size (10MB).
const DefaultMaxLogBytes int64 = 10 * 1024 * 1024

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithMaxLogBytes sets the maximum number of log bytes that will be downloaded
// and processed. Logs exceeding this limit will return ErrLogTooLarge.
// Set to 0 to disable the limit. Default is 10MB.
func WithMaxLogBytes(n int64) ClientOption {
	return func(c *Client) {
		c.maxLogBytes = n
	}
}

// Hook function types for different stages of downloadAndCacheWithBlobStorage
type AfterCacheCheckFunc func(ctx context.Context, result *CacheCheckResult)
type AfterJobStatusFunc func(ctx context.Context, result *JobStatusResult)
type AfterLogDownloadFunc func(ctx context.Context, result *LogDownloadResult)
type AfterLogParsingFunc func(ctx context.Context, result *LogParsingResult)
type AfterBlobStorageFunc func(ctx context.Context, result *BlobStorageResult)
type AfterLocalCacheFunc func(ctx context.Context, result *LocalCacheResult)

// Stage identifies the processing stage reported by hooks.
type Stage string

const (
	StageCacheCheck  Stage = "cache_check"
	StageJobStatus   Stage = "job_status"
	StageLogDownload Stage = "log_download"
	StageLogParsing  Stage = "log_parsing"
	StageBlobStorage Stage = "blob_storage"
	StageLocalCache  Stage = "local_cache"
)

// Hooks contains all registered hook functions
type Hooks struct {
	OnAfterCacheCheck  []AfterCacheCheckFunc
	OnAfterJobStatus   []AfterJobStatusFunc
	OnAfterLogDownload []AfterLogDownloadFunc
	OnAfterLogParsing  []AfterLogParsingFunc
	OnAfterBlobStorage []AfterBlobStorageFunc
	OnAfterLocalCache  []AfterLocalCacheFunc
}

// BaseResult contains common fields for all hook results
type BaseResult struct {
	Org, Pipeline, Build, Job string
	Duration                  time.Duration
	Stage                     Stage
	Success                   bool
	Err                       error
}

// CacheCheckResult contains the result of checking blob storage cache
type CacheCheckResult struct {
	BaseResult
	BlobKey string
	Exists  bool
}

// JobStatusResult contains the result of fetching job status
type JobStatusResult struct {
	BaseResult
	JobStatus *JobStatus
}

// LogDownloadResult contains the result of downloading logs from API
type LogDownloadResult struct {
	BaseResult
	LogSize int64 // Size of downloaded logs in bytes
}

// LogParsingResult contains the result of parsing logs to Parquet
type LogParsingResult struct {
	BaseResult
	ParquetSize int64 // Size of generated Parquet data in bytes
	LogEntries  int   // Number of log entries processed
}

// BlobStorageResult contains the result of storing data in blob storage
type BlobStorageResult struct {
	BaseResult
	BlobKey    string
	DataSize   int64
	IsTerminal bool
	TTL        time.Duration
}

// LocalCacheResult contains the result of creating local cache file
type LocalCacheResult struct {
	BaseResult
	LocalPath string
	FileSize  int64
}

// Hook registration methods
func (h *Hooks) AddAfterCacheCheck(hook AfterCacheCheckFunc) {
	h.OnAfterCacheCheck = append(h.OnAfterCacheCheck, hook)
}

func (h *Hooks) AddAfterJobStatus(hook AfterJobStatusFunc) {
	h.OnAfterJobStatus = append(h.OnAfterJobStatus, hook)
}

func (h *Hooks) AddAfterLogDownload(hook AfterLogDownloadFunc) {
	h.OnAfterLogDownload = append(h.OnAfterLogDownload, hook)
}

func (h *Hooks) AddAfterLogParsing(hook AfterLogParsingFunc) {
	h.OnAfterLogParsing = append(h.OnAfterLogParsing, hook)
}

func (h *Hooks) AddAfterBlobStorage(hook AfterBlobStorageFunc) {
	h.OnAfterBlobStorage = append(h.OnAfterBlobStorage, hook)
}

func (h *Hooks) AddAfterLocalCache(hook AfterLocalCacheFunc) {
	h.OnAfterLocalCache = append(h.OnAfterLocalCache, hook)
}

// Client provides a high-level convenience API for common buildkite-logs-parquet operations
type Client struct {
	api          BuildkiteAPI
	storageURL   string
	blobStorage  *BlobStorage
	hooks        *Hooks
	maxLogBytes  int64 // 0 means no limit
	refreshGroup singleflight.Group
}

// NewClient creates a new Client using the provided go-buildkite client
func NewClient(ctx context.Context, client *buildkite.Client, storageURL string, opts ...ClientOption) (*Client, error) {
	api := NewBuildkiteAPIExistingClient(client)
	return NewClientWithAPI(ctx, api, storageURL, opts...)
}

// NewClientWithAPI creates a new Client using a custom BuildkiteAPI implementation
func NewClientWithAPI(ctx context.Context, api BuildkiteAPI, storageURL string, opts ...ClientOption) (*Client, error) {
	// Initialize blob storage once during client creation
	blobStorage, err := NewBlobStorage(ctx, storageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blob storage: %w", err)
	}

	c := &Client{
		api:         api,
		storageURL:  storageURL,
		blobStorage: blobStorage,
		hooks:       &Hooks{},
		maxLogBytes: DefaultMaxLogBytes,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// NewReader downloads and caches job logs (if needed) and returns a ParquetReader for querying.
// The returned reader owns the underlying temp file; callers must call Close() when done.
//
// Parameters:
//   - org: Buildkite organization slug
//   - pipeline: Pipeline slug
//   - build: Build number or UUID
//   - job: Job ID
//   - ttl: Time-to-live for cache (use 0 for default 30s)
//   - forceRefresh: If true, forces re-download even if cache exists
func (c *Client) NewReader(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (*ParquetReader, error) {
	filePath, err := c.downloadAndCache(ctx, c.api, org, pipeline, build, job, ttl, forceRefresh)
	if err != nil {
		return nil, err
	}

	return newParquetReaderOwned(filePath), nil
}

// NewReaderByJobID downloads and caches job logs using only an organization slug and job UUID.
// Pipeline and build identifiers are resolved from the job's build_url so cache keys remain
// compatible with pipeline-scoped readers.
func (c *Client) NewReaderByJobID(ctx context.Context, org, job string, ttl time.Duration, forceRefresh bool) (*ParquetReader, error) {
	scoped, ok := c.api.(OrgScopedJobAPI)
	if !ok {
		return nil, fmt.Errorf("API client does not support organization-scoped job access")
	}

	location, err := ResolveJobLocation(ctx, scoped, org, job)
	if err != nil {
		return nil, err
	}

	adapter := &orgJobReaderAPI{
		base:     scoped,
		location: location,
	}

	filePath, err := c.downloadAndCache(ctx, adapter, location.Org, location.Pipeline, location.Build, location.Job, ttl, forceRefresh)
	if err != nil {
		return nil, err
	}

	return newParquetReaderOwned(filePath), nil
}

// downloadAndCache downloads and caches job logs as Parquet format, returning the local file path.
// Callers are responsible for removing the returned temp file.
func (c *Client) downloadAndCache(ctx context.Context, api BuildkiteAPI, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (string, error) {
	if err := ValidateAPIParams(org, pipeline, build, job); err != nil {
		return "", err
	}

	return c.downloadAndCacheWithBlobStorage(ctx, api, org, pipeline, build, job, ttl, forceRefresh)
}

// Hooks returns the hooks instance for registering callback functions
func (c *Client) Hooks() *Hooks {
	return c.hooks
}

// downloadAndCacheWithBlobStorage downloads logs using the client's blob storage backend
func (c *Client) downloadAndCacheWithBlobStorage(ctx context.Context, api BuildkiteAPI, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (string, error) {
	if ttl == 0 {
		ttl = 30 * time.Second // Default TTL
	}

	blobKey := GenerateBlobKey(org, pipeline, build, job)

	cacheCheckStart := time.Now()
	exists, err := c.blobStorage.Exists(ctx, blobKey)
	cacheCheckDuration := time.Since(cacheCheckStart)
	c.fireCacheCheckHook(ctx, org, pipeline, build, job, cacheCheckDuration, blobKey, exists, err)
	if err != nil {
		return "", fmt.Errorf("failed to check blob existence: %w", err)
	}

	if exists && !forceRefresh {
		if usable, err := c.cacheUsable(ctx, blobKey, ttl); err == nil && usable {
			return c.createLocalCacheFileWithHooks(ctx, org, pipeline, build, job, blobKey)
		}
	}

	// Decouple shared refresh work from the single caller that wins the
	// singleflight race. Waiters can still abandon their own wait below.
	refreshCtx := context.WithoutCancel(ctx)
	inflightKey := blobKey
	ch := c.refreshGroup.DoChan(inflightKey, func() (any, error) {
		if !forceRefresh {
			if usable, err := c.cacheUsable(refreshCtx, blobKey, ttl); err == nil && usable {
				return nil, nil
			}
		}
		return nil, c.refreshBlobCache(refreshCtx, api, org, pipeline, build, job, ttl, blobKey)
	})

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-ch:
		if result.Err != nil {
			return "", result.Err
		}
	}

	return c.createLocalCacheFileWithHooks(ctx, org, pipeline, build, job, blobKey)
}

func (c *Client) cacheUsable(ctx context.Context, blobKey string, ttl time.Duration) (bool, error) {
	metadata, err := c.blobStorage.ReadWithMetadata(ctx, blobKey)
	if err != nil || metadata == nil {
		return false, err
	}
	if metadata.IsTerminal {
		return true, nil
	}

	return time.Since(metadata.CachedAt) <= ttl, nil
}

func (c *Client) refreshBlobCache(ctx context.Context, api BuildkiteAPI, org, pipeline, build, job string, ttl time.Duration, blobKey string) error {
	jobStatusStart := time.Now()
	jobStatus, err := api.GetJobStatus(ctx, org, pipeline, build, job)
	jobStatusDuration := time.Since(jobStatusStart)
	c.fireJobStatusHook(ctx, org, pipeline, build, job, jobStatusDuration, jobStatus, err)
	if err != nil {
		return fmt.Errorf("failed to get job status: %w", err)
	}

	logDownloadStart := time.Now()
	logReader, err := api.GetJobLog(ctx, org, pipeline, build, job)
	logDownloadDuration := time.Since(logDownloadStart)
	if err != nil {
		c.fireLogDownloadHook(ctx, org, pipeline, build, job, logDownloadDuration, 0, err)
		return fmt.Errorf("failed to fetch logs from API: %w", err)
	}
	defer logReader.Close()

	var logSize int64
	if seeker, ok := logReader.(io.Seeker); ok {
		if size, seekErr := seeker.Seek(0, io.SeekEnd); seekErr == nil {
			logSize = size
			if _, resetErr := seeker.Seek(0, io.SeekStart); resetErr != nil {
				logSize = 0
			}
		}
	}

	countingReader := &countingReadCloser{rc: logReader}
	logReader = countingReader
	if c.maxLogBytes > 0 {
		if logSize > c.maxLogBytes {
			err := fmt.Errorf("%w: %d bytes exceeds limit of %d bytes", ErrLogTooLarge, logSize, c.maxLogBytes)
			c.fireLogDownloadHook(ctx, org, pipeline, build, job, logDownloadDuration, logSize, err)
			return err
		}
		limitedReader := &limitedReadCloser{
			rc:    logReader,
			r:     io.LimitReader(logReader, c.maxLogBytes+1),
			limit: c.maxLogBytes,
		}
		logReader = limitedReader
	}

	logParsingStart := time.Now()
	parser := NewParser()
	tempFile, err := os.CreateTemp("", "bklog-*.parquet")
	if err != nil {
		logParsingDuration := time.Since(logParsingStart)
		c.fireLogParsingHook(ctx, org, pipeline, build, job, logParsingDuration, 0, 0, err)
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		logParsingDuration := time.Since(logParsingStart)
		c.fireLogParsingHook(ctx, org, pipeline, build, job, logParsingDuration, 0, 0, err)
		return fmt.Errorf("failed to close temp file before export: %w", err)
	}
	defer func() {
		_ = os.Remove(tempPath)
	}()

	logEntries, err := ExportSeq2ToParquetWithFilterAndStats(parser.All(logReader), tempPath, nil)
	logParsingDuration := time.Since(logParsingStart)
	if err != nil {
		if isLogDownloadError(err) {
			if logSize == 0 {
				logSize = countingReader.consumed
			}
			c.fireLogDownloadHook(ctx, org, pipeline, build, job, time.Since(logDownloadStart), logSize, err)
			return fmt.Errorf("failed to fetch logs from API: %w", err)
		}
		c.fireLogParsingHook(ctx, org, pipeline, build, job, logParsingDuration, 0, logEntries, err)
		return fmt.Errorf("failed to export logs to parquet: %w", err)
	}
	fileInfo, err := os.Stat(tempPath) //nolint:gosec // path from os.CreateTemp, not user input
	if err != nil {
		c.fireLogParsingHook(ctx, org, pipeline, build, job, logParsingDuration, 0, logEntries, err)
		return fmt.Errorf("failed to measure parquet data: %w", err)
	}
	parquetSize := fileInfo.Size()
	if logSize == 0 {
		logSize = countingReader.consumed
	}
	c.fireLogDownloadHook(ctx, org, pipeline, build, job, time.Since(logDownloadStart), logSize, nil)
	c.fireLogParsingHook(ctx, org, pipeline, build, job, logParsingDuration, parquetSize, logEntries, nil)

	blobStorageStart := time.Now()
	metadata := &BlobMetadata{
		JobID:        job,
		JobState:     string(jobStatus.State),
		IsTerminal:   jobStatus.IsTerminal,
		CachedAt:     time.Now(),
		TTL:          ttl.String(),
		Organization: org,
		Pipeline:     pipeline,
		Build:        build,
		LogSize:      logSize,
		ParquetSize:  parquetSize,
		RowCount:     logEntries,
		ProcessedAt:  time.Now(),
	}
	parquetReader, err := os.Open(tempPath) //nolint:gosec // path from os.CreateTemp, not user input
	if err != nil {
		blobStorageDuration := time.Since(blobStorageStart)
		c.fireBlobStorageHook(ctx, org, pipeline, build, job, blobStorageDuration, blobKey, parquetSize, jobStatus.IsTerminal, ttl, err)
		return fmt.Errorf("failed to open parquet data: %w", err)
	}
	defer parquetReader.Close()

	err = c.blobStorage.WriteWithMetadataFrom(ctx, blobKey, parquetReader, metadata)
	blobStorageDuration := time.Since(blobStorageStart)
	c.fireBlobStorageHook(ctx, org, pipeline, build, job, blobStorageDuration, blobKey, parquetSize, jobStatus.IsTerminal, ttl, err)
	if err != nil {
		return fmt.Errorf("failed to write to blob storage: %w", err)
	}

	return nil
}

func (c *Client) createLocalCacheFileWithHooks(ctx context.Context, org, pipeline, build, job, blobKey string) (string, error) {
	localCacheStart := time.Now()
	localPath, err := createLocalCacheFile(ctx, c.blobStorage, blobKey)
	localCacheDuration := time.Since(localCacheStart)

	var fileSize int64
	if err == nil {
		if stat, statErr := os.Stat(localPath); statErr == nil { //nolint:gosec // path from createLocalCacheFile, not user input
			fileSize = stat.Size()
		}
	}

	c.fireLocalCacheHook(ctx, org, pipeline, build, job, localCacheDuration, localPath, fileSize, err)
	if err != nil {
		return "", fmt.Errorf("failed to create local cache file: %w", err)
	}

	return localPath, nil
}

func (c *Client) fireCacheCheckHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, blobKey string, exists bool, err error) {
	for _, hook := range c.hooks.OnAfterCacheCheck {
		hook(ctx, &CacheCheckResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageCacheCheck,
				Success:  err == nil,
				Err:      err,
			},
			BlobKey: blobKey,
			Exists:  exists,
		})
	}
}

func (c *Client) fireJobStatusHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, jobStatus *JobStatus, err error) {
	for _, hook := range c.hooks.OnAfterJobStatus {
		hook(ctx, &JobStatusResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageJobStatus,
				Success:  err == nil,
				Err:      err,
			},
			JobStatus: jobStatus,
		})
	}
}

func (c *Client) fireLogDownloadHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, logSize int64, err error) {
	for _, hook := range c.hooks.OnAfterLogDownload {
		hook(ctx, &LogDownloadResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageLogDownload,
				Success:  err == nil,
				Err:      err,
			},
			LogSize: logSize,
		})
	}
}

func (c *Client) fireLogParsingHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, parquetSize int64, logEntries int, err error) {
	for _, hook := range c.hooks.OnAfterLogParsing {
		hook(ctx, &LogParsingResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageLogParsing,
				Success:  err == nil,
				Err:      err,
			},
			ParquetSize: parquetSize,
			LogEntries:  logEntries,
		})
	}
}

func (c *Client) fireBlobStorageHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, blobKey string, dataSize int64, isTerminal bool, ttl time.Duration, err error) {
	for _, hook := range c.hooks.OnAfterBlobStorage {
		hook(ctx, &BlobStorageResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageBlobStorage,
				Success:  err == nil,
				Err:      err,
			},
			BlobKey:    blobKey,
			DataSize:   dataSize,
			IsTerminal: isTerminal,
			TTL:        ttl,
		})
	}
}

func (c *Client) fireLocalCacheHook(ctx context.Context, org, pipeline, build, job string, duration time.Duration, localPath string, fileSize int64, err error) {
	for _, hook := range c.hooks.OnAfterLocalCache {
		hook(ctx, &LocalCacheResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: duration,
				Stage:    StageLocalCache,
				Success:  err == nil,
				Err:      err,
			},
			LocalPath: localPath,
			FileSize:  fileSize,
		})
	}
}

type logDownloadError struct {
	err error
}

func (e *logDownloadError) Error() string {
	return e.err.Error()
}

func (e *logDownloadError) Unwrap() error {
	return e.err
}

func isLogDownloadError(err error) bool {
	var downloadErr *logDownloadError
	return errors.As(err, &downloadErr) || errors.Is(err, ErrLogTooLarge)
}

// countingReadCloser tracks bytes consumed from a streaming log reader.
type countingReadCloser struct {
	rc       io.ReadCloser
	consumed int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	c.consumed += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}

// limitedReadCloser wraps a reader with a size limit, returning ErrLogTooLarge
// if the limit is exceeded during reading.
type limitedReadCloser struct {
	rc       io.ReadCloser
	r        io.Reader // LimitReader set to limit+1
	limit    int64
	consumed int64
}

func (l *limitedReadCloser) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	l.consumed += int64(n)
	if l.consumed > l.limit {
		return n, fmt.Errorf("%w: exceeded limit of %d bytes", ErrLogTooLarge, l.limit)
	}
	return n, err
}

func (l *limitedReadCloser) Close() error {
	return l.rc.Close()
}

// Close closes the underlying blob storage connection
func (c *Client) Close() error {
	if c.blobStorage != nil {
		return c.blobStorage.Close()
	}
	return nil
}
