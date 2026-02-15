package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/buildkite/go-buildkite/v4"
)

// Hook function types for different stages of downloadAndCacheWithBlobStorage
type AfterCacheCheckFunc func(ctx context.Context, result *CacheCheckResult)
type AfterJobStatusFunc func(ctx context.Context, result *JobStatusResult)
type AfterLogDownloadFunc func(ctx context.Context, result *LogDownloadResult)
type AfterLogParsingFunc func(ctx context.Context, result *LogParsingResult)
type AfterBlobStorageFunc func(ctx context.Context, result *BlobStorageResult)
type AfterLocalCacheFunc func(ctx context.Context, result *LocalCacheResult)

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
	api         BuildkiteAPI
	storageURL  string
	blobStorage *BlobStorage
	hooks       *Hooks
}

// NewClient creates a new Client using the provided go-buildkite client
func NewClient(ctx context.Context, client *buildkite.Client, storageURL string) (*Client, error) {
	api := NewBuildkiteAPIExistingClient(client)
	return NewClientWithAPI(ctx, api, storageURL)
}

// NewClientWithAPI creates a new Client using a custom BuildkiteAPI implementation
func NewClientWithAPI(ctx context.Context, api BuildkiteAPI, storageURL string) (*Client, error) {
	// Initialize blob storage once during client creation
	blobStorage, err := NewBlobStorage(ctx, storageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blob storage: %w", err)
	}

	return &Client{
		api:         api,
		storageURL:  storageURL,
		blobStorage: blobStorage,
		hooks:       &Hooks{},
	}, nil
}

// DownloadAndCache downloads and caches job logs as Parquet format, returning the local file path
//
// Parameters:
//   - org: Buildkite organization slug
//   - pipeline: Pipeline slug
//   - build: Build number or UUID
//   - job: Job ID
//   - ttl: Time-to-live for cache (use 0 for default 30s)
//   - forceRefresh: If true, forces re-download even if cache exists
//
// Returns the local file path of the cached Parquet file
func (c *Client) DownloadAndCache(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (string, error) {
	if err := ValidateAPIParams(org, pipeline, build, job); err != nil {
		return "", err
	}

	return c.downloadAndCacheWithBlobStorage(ctx, org, pipeline, build, job, ttl, forceRefresh)
}

// NewReader downloads and caches job logs (if needed) and returns a ParquetReader for querying
//
// Parameters:
//   - org: Buildkite organization slug
//   - pipeline: Pipeline slug
//   - build: Build number or UUID
//   - job: Job ID
//   - ttl: Time-to-live for cache (use 0 for default 30s)
//   - forceRefresh: If true, forces re-download even if cache exists
//
// Returns a ParquetReader for querying the log data
func (c *Client) NewReader(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (*ParquetReader, error) {
	filePath, err := c.DownloadAndCache(ctx, org, pipeline, build, job, ttl, forceRefresh)
	if err != nil {
		return nil, err
	}

	return NewParquetReader(filePath), nil
}

// Hooks returns the hooks instance for registering callback functions
func (c *Client) Hooks() *Hooks {
	return c.hooks
}

// downloadAndCacheWithBlobStorage downloads logs using the client's blob storage backend
func (c *Client) downloadAndCacheWithBlobStorage(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (string, error) {
	if ttl == 0 {
		ttl = 30 * time.Second // Default TTL
	}

	blobKey := GenerateBlobKey(org, pipeline, build, job)

	// Check if blob already exists
	cacheCheckStart := time.Now()
	exists, err := c.blobStorage.Exists(ctx, blobKey)
	if err != nil {
		return "", fmt.Errorf("failed to check blob existence: %w", err)
	}

	cacheCheckDuration := time.Since(cacheCheckStart)

	// Call after cache check hooks
	for _, hook := range c.hooks.OnAfterCacheCheck {
		hook(ctx, &CacheCheckResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: cacheCheckDuration,
			},
			BlobKey: blobKey,
			Exists:  exists,
		})
	}

	// Get job status to determine caching strategy
	jobStatusStart := time.Now()
	jobStatus, err := c.api.GetJobStatus(ctx, org, pipeline, build, job)
	if err != nil {
		return "", fmt.Errorf("failed to get job status: %w", err)
	}

	jobStatusDuration := time.Since(jobStatusStart)

	// Call after job status hooks
	for _, hook := range c.hooks.OnAfterJobStatus {
		hook(ctx, &JobStatusResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: jobStatusDuration,
			},
			JobStatus: jobStatus,
		})
	}

	// Check if we should use existing cache
	if exists && !forceRefresh {
		metadata, err := c.blobStorage.ReadWithMetadata(ctx, blobKey)
		if err == nil && metadata != nil {
			// For terminal jobs, always use cache
			if metadata.IsTerminal {
				return createLocalCacheFile(ctx, c.blobStorage, blobKey)
			}

			// For non-terminal jobs, check TTL
			timeElapsed := time.Since(metadata.CachedAt)
			if timeElapsed < ttl {
				return createLocalCacheFile(ctx, c.blobStorage, blobKey)
			}
		}
	}

	// Download fresh logs from API
	logDownloadStart := time.Now()
	logReader, err := c.api.GetJobLog(ctx, org, pipeline, build, job)
	if err != nil {
		return "", fmt.Errorf("failed to fetch logs from API: %w", err)
	}
	defer logReader.Close()

	logDownloadDuration := time.Since(logDownloadStart)

	var logSize int64
	if logReader != nil {
		// Get content length if available
		if seeker, ok := logReader.(io.Seeker); ok {
			if size, seekErr := seeker.Seek(0, 2); seekErr == nil {
				logSize = size
				if _, resetErr := seeker.Seek(0, 0); resetErr != nil {
					// If we can't reset, continue anyway - the reader might still work
					logSize = 0
				}
			}
		}
	}

	// Call after log download hooks
	for _, hook := range c.hooks.OnAfterLogDownload {
		hook(ctx, &LogDownloadResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: logDownloadDuration,
			},
			LogSize: logSize,
		})
	}

	// Parse logs and convert to parquet data
	logParsingStart := time.Now()
	parser := NewParser()
	var parquetData []byte

	// Create a temporary file for parquet export
	tempFile, err := os.CreateTemp("", "bklog-*.parquet")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() {
		tempFile.Close()
		os.Remove(tempPath)
	}()

	// Export logs to parquet using streaming approach
	if err := ExportSeq2ToParquet(parser.All(logReader), tempPath); err != nil {
		logParsingDuration := time.Since(logParsingStart)

		// Call after log parsing hooks (with error)
		for _, hook := range c.hooks.OnAfterLogParsing {
			hook(ctx, &LogParsingResult{
				BaseResult: BaseResult{
					Org:      org,
					Pipeline: pipeline,
					Build:    build,
					Job:      job,
					Duration: logParsingDuration,
				},
				ParquetSize: 0,
				LogEntries:  0,
			})
		}
		return "", fmt.Errorf("failed to export logs to parquet: %w", err)
	}

	err = tempFile.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	// Read the parquet data
	parquetData, err = os.ReadFile(tempPath)
	if err != nil {
		return "", fmt.Errorf("failed to read parquet data: %w", err)
	}

	logParsingDuration := time.Since(logParsingStart)

	// Call after log parsing hooks
	for _, hook := range c.hooks.OnAfterLogParsing {
		hook(ctx, &LogParsingResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: logParsingDuration,
			},
			ParquetSize: int64(len(parquetData)),
			LogEntries:  0, // Consumer can count if needed
		})
	}

	// Store in blob storage with metadata
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
	}

	err = c.blobStorage.WriteWithMetadata(ctx, blobKey, parquetData, metadata)
	if err != nil {
		return "", fmt.Errorf("failed to write to blob storage: %w", err)
	}

	blobStorageDuration := time.Since(blobStorageStart)

	// Call after blob storage hooks
	for _, hook := range c.hooks.OnAfterBlobStorage {
		hook(ctx, &BlobStorageResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: blobStorageDuration,
			},
			BlobKey:    blobKey,
			DataSize:   int64(len(parquetData)),
			IsTerminal: jobStatus.IsTerminal,
			TTL:        ttl,
		})
	}

	// Create local cache file directly from in-memory data,
	// avoiding a redundant blob storage read.
	localCacheStart := time.Now()
	localPath, err := createLocalCacheFileFromData(parquetData)
	if err != nil {
		return "", fmt.Errorf("failed to create local cache file: %w", err)
	}

	localCacheDuration := time.Since(localCacheStart)

	var fileSize int64
	if stat, statErr := os.Stat(localPath); statErr == nil {
		fileSize = stat.Size()
	}

	// Call after local cache hooks
	for _, hook := range c.hooks.OnAfterLocalCache {
		hook(ctx, &LocalCacheResult{
			BaseResult: BaseResult{
				Org:      org,
				Pipeline: pipeline,
				Build:    build,
				Job:      job,
				Duration: localCacheDuration,
			},
			LocalPath: localPath,
			FileSize:  fileSize,
		})
	}

	return localPath, nil
}

// Close closes the underlying blob storage connection
func (c *Client) Close() error {
	if c.blobStorage != nil {
		return c.blobStorage.Close()
	}
	return nil
}
