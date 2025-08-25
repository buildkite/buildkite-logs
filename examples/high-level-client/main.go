package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	buildkitelogs "github.com/buildkite/buildkite-logs"
	"github.com/buildkite/go-buildkite/v4"
)

func main() {
	// Get API token from environment
	apiToken := os.Getenv("BUILDKITE_API_TOKEN")
	if apiToken == "" {
		log.Fatal("BUILDKITE_API_TOKEN environment variable is required")
	}

	// Create buildkite client
	client, err := buildkite.NewOpts(buildkite.WithTokenAuth(apiToken))
	if err != nil {
		log.Fatal("Failed to create buildkite client:", err)
	}

	ctx := context.Background()

	// Create high-level Client
	storageURL := "file://~/.bklog" // Uses default storage location
	buildkiteLogsClient, err := buildkitelogs.NewClient(ctx, client, storageURL)
	if err != nil {
		log.Fatal("Failed to create buildkite logs client:", err)
	}
	defer buildkiteLogsClient.Close()

	// Setup hooks for observability
	buildkiteLogsClient.Hooks().AddAfterCacheCheck(func(ctx context.Context, result *buildkitelogs.CacheCheckResult) {
		log.Printf("🔍 Cache check for %s: exists=%t, took %v", result.BlobKey, result.Exists, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterJobStatus(func(ctx context.Context, result *buildkitelogs.JobStatusResult) {
		log.Printf("✅ Job status: %s (terminal: %t) - took %v",
			result.JobStatus.State, result.JobStatus.IsTerminal, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterLogDownload(func(ctx context.Context, result *buildkitelogs.LogDownloadResult) {
		log.Printf("⬇️  Downloaded %d bytes in %v", result.LogSize, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterLogParsing(func(ctx context.Context, result *buildkitelogs.LogParsingResult) {
		log.Printf("🔄 Parsed logs to %d bytes Parquet in %v", result.ParquetSize, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterBlobStorage(func(ctx context.Context, result *buildkitelogs.BlobStorageResult) {
		log.Printf("💾 Stored %d bytes to blob storage (terminal: %t) in %v",
			result.DataSize, result.IsTerminal, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterLocalCache(func(ctx context.Context, result *buildkitelogs.LocalCacheResult) {
		log.Printf("📁 Created local cache file %s (%d bytes) in %v",
			result.LocalPath, result.FileSize, result.Duration)
	})

	org := "myorg"
	pipeline := "mypipeline"
	build := "123"
	job := "abc-123-def"

	// Example 1: Just download and cache logs
	fmt.Println("Downloading and caching logs...")
	filePath, err := buildkiteLogsClient.DownloadAndCache(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Printf("Failed to download and cache: %v", err)
		return
	}
	fmt.Printf("Cached to: %s\n", filePath)

	// Example 2: Get a reader and query the logs
	fmt.Println("Creating reader and querying logs...")
	reader, err := buildkiteLogsClient.NewReader(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Printf("Failed to create reader: %v", err)
		return
	}

	// Get file info
	info, err := reader.GetFileInfo()
	if err != nil {
		log.Printf("Failed to get file info: %v", err)
		return
	}
	fmt.Printf("Log file contains %d rows\n", info.RowCount)

	// Read first 10 entries
	count := 0
	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			log.Printf("Error reading entries: %v", err)
			return
		}

		fmt.Printf("Entry %d: %s\n", count+1, entry.Content)
		count++
		if count >= 10 {
			break
		}
	}

	// Example 3: Using with custom API implementation
	fmt.Println("\nExample with custom API:")
	customAPI := &CustomBuildkiteAPI{} // Your custom implementation
	customClient, err := buildkitelogs.NewClientWithAPI(ctx, customAPI, storageURL)
	if err != nil {
		log.Printf("Failed to create custom client: %v", err)
		return
	}
	defer customClient.Close()

	reader2, err := customClient.NewReader(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Printf("Failed to create reader with custom API: %v", err)
		return
	}

	// Search for specific patterns
	searchOpts := buildkitelogs.SearchOptions{
		Pattern:       "error",
		Context:       2,
		CaseSensitive: false,
	}

	fmt.Println("Searching for 'error' in logs:")
	for result, err := range reader2.SearchEntriesIter(searchOpts) {
		if err != nil {
			log.Printf("Error searching: %v", err)
			return
		}
		fmt.Printf("Found error at row %d: %s\n", result.Match.RowNumber, result.Match.Content)
		break // Just show first result
	}
}

// CustomBuildkiteAPI is an example custom implementation
type CustomBuildkiteAPI struct {
	// Your custom implementation fields
}

func (c *CustomBuildkiteAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	// Your custom log fetching logic
	return nil, fmt.Errorf("not implemented")
}

func (c *CustomBuildkiteAPI) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*buildkitelogs.JobStatus, error) {
	// Your custom job status logic
	return nil, fmt.Errorf("not implemented")
}
