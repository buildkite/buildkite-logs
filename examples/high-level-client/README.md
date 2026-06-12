# High-Level Client API Example

This example demonstrates how to use the high-level `Client` API for downloading, caching, and querying Buildkite logs.

## Overview

The `Client` provides a simplified interface for common operations:

- Download and cache job logs from Buildkite API
- Automatic conversion to Parquet format for efficient querying
- Support for both official `*buildkite.Client` and custom `BuildkiteAPI` implementations
- Built-in caching with TTL and force refresh options
- Optional hooks system for observability and tracing

## Prerequisites

**Buildkite API Token**: Set the `BUILDKITE_API_TOKEN` environment variable:
```bash
export BUILDKITE_API_TOKEN="your-buildkite-api-token"
```

Optional overrides for the example job (defaults are placeholders):
```bash
export BUILDKITE_ORGANIZATION_SLUG="myorg"
export BUILDKITE_PIPELINE_SLUG="mypipeline"
export BUILDKITE_BUILD_NUMBER="123"
export BUILDKITE_JOB_ID="your-job-uuid"
```

## Running the Example

```bash
cd examples/high-level-client
go run main.go
```

## What the Example Does

The example demonstrates two main use cases plus hooks for observability:

### 1. Pipeline-Scoped Reader (`NewReader`)

Creates a `Client` using the official `*buildkite.Client` and shows how to:
- Download and cache logs to a local file
- Create a reader for querying the cached data
- Read basic file information (row count, file size, etc.)
- Iterate through log entries

Requires organization, pipeline, build, and job identifiers.

### 2. Job UUID-Only Reader (`NewReaderByJobID`)

Shows how to fetch and query logs when you only have an organization slug and job UUID:
- Uses organization-scoped REST endpoints (`/v2/organizations/{org}/jobs/{jobID}`)
- Resolves pipeline and build from the job's `build_url` for cache key compatibility
- Searches log entries with regex patterns

This matches how the Buildkite CLI accesses job logs by UUID alone.

### 3. Observability Hooks

Shows how to set up hooks for detailed observability of the download and caching process:
- Cache check timing and results
- Job status API call timing  
- Log download progress
- Parquet conversion performance
- Blob storage operations
- Local cache file creation

The example uses hooks to provide real-time feedback with emojis and timing information, making it easy to see what's happening at each stage.

## Configuration

### Storage URLs

The example uses `file://~/.bklog` as the storage URL, but you can use other backends:

- **Local filesystem**: `file://./cache` or `file:///absolute/path`
- **AWS S3**: `s3://bucket-name/path`
- **Google Cloud Storage**: `gs://bucket-name/path`
- **Azure Blob**: `azblob://container/path`

### Caching Behavior

- **TTL**: Set to 5 minutes in the example - logs are re-fetched if older than this
- **Force Refresh**: Set to `false` - change to `true` to always re-download
- **Terminal Jobs**: Jobs that are finished (passed/failed/canceled) are cached permanently

## Key Functions

### `NewClient(client, storageURL)`

Creates a client using the official go-buildkite client.

### `NewClientWithAPI(api, storageURL)`

Creates a client using a custom API implementation.

### `DownloadAndCache(ctx, org, pipeline, build, job, ttl, forceRefresh)`

Downloads and caches logs, returns the local file path.

### `NewReaderByJobID(ctx, org, job, ttl, forceRefresh)`

Downloads/caches logs using only org + job UUID and returns a `ParquetReader`.
Pipeline and build are resolved automatically from the job's `build_url`.

### `ResolveJobLocation(ctx, api, org, job)`

Resolves pipeline and build identifiers for a job UUID without downloading logs.

## Error Handling

The client provides automatic parameter validation:

```go
// This will return an error about missing organization
_, err := client.DownloadAndCache(ctx, "", "pipeline", "build", "job", 0, false)
```

## Integration with Other Examples

This high-level API works well with other examples in this repository:

- Use the cached Parquet files with the [query example](../query/) for advanced searching
- Integrate with the [smart-cache example](../smart-cache/) for more sophisticated caching strategies

## Next Steps

- Check out the [detailed API documentation](../../docs/client-api.md)
- See the main [README](../../README.md) for more usage patterns
- Explore the low-level API for advanced use cases
