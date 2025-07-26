package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/buildkite/go-buildkite/v4"
	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
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

	// Create high-level ParquetClient
	storageURL := "file://~/.bklog" // Uses default storage location
	parquetClient := buildkitelogs.NewParquetClient(client, storageURL)

	ctx := context.Background()
	org := "myorg"
	pipeline := "mypipeline"
	build := "123"
	job := "abc-123-def"

	// Example 1: Just download and cache logs
	fmt.Println("Downloading and caching logs...")
	filePath, err := parquetClient.DownloadAndCache(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Fatal("Failed to download and cache:", err)
	}
	fmt.Printf("Cached to: %s\n", filePath)

	// Example 2: Get a reader and query the logs
	fmt.Println("Creating reader and querying logs...")
	reader, err := parquetClient.NewReader(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Fatal("Failed to create reader:", err)
	}

	// Get file info
	info, err := reader.GetFileInfo()
	if err != nil {
		log.Fatal("Failed to get file info:", err)
	}
	fmt.Printf("Log file contains %d rows\n", info.RowCount)

	// Read first 10 entries
	count := 0
	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			log.Fatal("Error reading entries:", err)
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
	customClient := buildkitelogs.NewParquetClientWithAPI(customAPI, storageURL)

	reader2, err := customClient.NewReader(ctx, org, pipeline, build, job, time.Minute*5, false)
	if err != nil {
		log.Fatal("Failed to create reader with custom API:", err)
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
			log.Fatal("Error searching:", err)
		}
		fmt.Printf("Found error at row %d: %s\n", result.LineNumber, result.Match.Content)
		break // Just show first result
	}
}

// CustomBuildkiteAPI is an example custom implementation
type CustomBuildkiteAPI struct {
	// Your custom implementation fields
}

func (c *CustomBuildkiteAPI) GetJobLog(org, pipeline, build, job string) (io.ReadCloser, error) {
	// Your custom log fetching logic
	return nil, fmt.Errorf("not implemented")
}

func (c *CustomBuildkiteAPI) GetJobStatus(org, pipeline, build, job string) (*buildkitelogs.JobStatus, error) {
	// Your custom job status logic
	return nil, fmt.Errorf("not implemented")
}
