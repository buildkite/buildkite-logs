package buildkitelogs

import (
	"context"
	"time"

	"github.com/buildkite/go-buildkite/v4"
)

// ParquetClient provides a high-level convenience API for common buildkite-logs-parquet operations
type ParquetClient struct {
	api        BuildkiteAPI
	storageURL string
}

// NewParquetClient creates a new ParquetClient using the provided go-buildkite client
func NewParquetClient(client *buildkite.Client, storageURL string) *ParquetClient {
	api := NewBuildkiteAPIExistingClient(client)
	return &ParquetClient{
		api:        api,
		storageURL: storageURL,
	}
}

// NewParquetClientWithAPI creates a new ParquetClient using a custom BuildkiteAPI implementation
func NewParquetClientWithAPI(api BuildkiteAPI, storageURL string) *ParquetClient {
	return &ParquetClient{
		api:        api,
		storageURL: storageURL,
	}
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
func (c *ParquetClient) DownloadAndCache(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (string, error) {
	if err := ValidateAPIParams(org, pipeline, build, job); err != nil {
		return "", err
	}

	return DownloadAndCacheBlobStorage(ctx, c.api, org, pipeline, build, job, c.storageURL, ttl, forceRefresh)
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
func (c *ParquetClient) NewReader(ctx context.Context, org, pipeline, build, job string, ttl time.Duration, forceRefresh bool) (*ParquetReader, error) {
	filePath, err := c.DownloadAndCache(ctx, org, pipeline, build, job, ttl, forceRefresh)
	if err != nil {
		return nil, err
	}

	return NewParquetReader(filePath), nil
}
