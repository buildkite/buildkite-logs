package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	buildkitelogs "github.com/buildkite/buildkite-logs"
	"github.com/buildkite/go-buildkite/v5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	apiToken := os.Getenv("BUILDKITE_API_TOKEN")
	if apiToken == "" {
		return fmt.Errorf("BUILDKITE_API_TOKEN environment variable is required")
	}

	client, err := buildkite.NewOpts(buildkite.WithTokenAuth(apiToken))
	if err != nil {
		return fmt.Errorf("failed to create buildkite client: %w", err)
	}

	storageURL := "file://~/.bklog"
	buildkiteLogsClient, err := buildkitelogs.NewClient(ctx, client, storageURL)
	if err != nil {
		return fmt.Errorf("failed to create buildkite logs client: %w", err)
	}
	defer buildkiteLogsClient.Close()

	buildkiteLogsClient.Hooks().AddAfterCacheCheck(func(ctx context.Context, result *buildkitelogs.CacheCheckResult) {
		log.Printf("Cache check for %s: exists=%t, took %v", result.BlobKey, result.Exists, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterJobStatus(func(ctx context.Context, result *buildkitelogs.JobStatusResult) {
		log.Printf("Job status: %s (terminal: %t) - took %v",
			result.JobStatus.State, result.JobStatus.IsTerminal, result.Duration)
	})

	buildkiteLogsClient.Hooks().AddAfterLogDownload(func(ctx context.Context, result *buildkitelogs.LogDownloadResult) {
		log.Printf("Downloaded %d bytes in %v", result.LogSize, result.Duration)
	})

	org := envOrDefault("BUILDKITE_ORGANIZATION_SLUG", "myorg")
	pipeline := envOrDefault("BUILDKITE_PIPELINE_SLUG", "mypipeline")
	build := envOrDefault("BUILDKITE_BUILD_NUMBER", "123")
	job := envOrDefault("BUILDKITE_JOB_ID", "abc-123-def")

	// Example 1: pipeline-scoped reader (org + pipeline + build + job)
	fmt.Println("Example 1: NewReader with pipeline and build context...")
	reader, err := buildkiteLogsClient.NewReader(ctx, org, pipeline, build, job, 5*time.Minute, false)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Close()

	info, err := reader.GetFileInfo()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	fmt.Printf("Log file contains %d rows\n", info.RowCount)

	count := 0
	for entry, err := range reader.ReadEntriesIter(ctx) {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		fmt.Printf("Entry %d: %s\n", count+1, entry.Content)
		count++
		if count >= 10 {
			break
		}
	}

	// Example 2: org-scoped reader (org + job UUID only)
	// Uses GET /v2/organizations/{org}/jobs/{jobID} and .../log under the hood.
	// Pipeline and build are resolved from build_url so cache keys match Example 1.
	fmt.Println("\nExample 2: NewReaderByJobID with org and job UUID only...")
	location, err := buildkitelogs.ResolveJobLocation(ctx, buildkitelogs.NewBuildkiteAPIExistingClient(client), org, job)
	if err != nil {
		return fmt.Errorf("failed to resolve job location: %w", err)
	}
	fmt.Printf("Resolved pipeline=%s build=%s for job %s\n", location.Pipeline, location.Build, location.Job)

	readerByJob, err := buildkiteLogsClient.NewReaderByJobID(ctx, org, job, 5*time.Minute, false)
	if err != nil {
		return fmt.Errorf("failed to create reader by job ID: %w", err)
	}
	defer readerByJob.Close()

	searchOpts := buildkitelogs.SearchOptions{
		Pattern:       "error",
		Context:       2,
		CaseSensitive: false,
	}

	fmt.Println("Searching for 'error' in logs:")
	found := false
	for result, err := range readerByJob.SearchEntriesIter(ctx, searchOpts) {
		if err != nil {
			return fmt.Errorf("error searching: %w", err)
		}
		fmt.Printf("Found error at row %d: %s\n", result.Match.RowNumber, result.Match.Content)
		found = true
		break
	}
	if !found {
		fmt.Println("No matches found")
	}

	return nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
