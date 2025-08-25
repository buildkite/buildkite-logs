package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/buildkite/go-buildkite/v4"
)

// JobStatusProvider defines the interface for getting job status
type JobStatusProvider interface {
	GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error)
}

// LogProvider defines the interface for getting job logs
type LogProvider interface {
	GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error)
}

// BuildkiteAPI combines both job status and log providers
type BuildkiteAPI interface {
	JobStatusProvider
	LogProvider
}

// BuildkiteAPIClient provides methods to interact with the Buildkite API
// Now wraps the official go-buildkite v4 client
type BuildkiteAPIClient struct {
	client *buildkite.Client
}

// NewBuildkiteAPIClient creates a new Buildkite API client using go-buildkite v4
func NewBuildkiteAPIClient(apiToken, version string) *BuildkiteAPIClient {
	userAgent := fmt.Sprintf("buildkite-logs-parquet/%s (Go; %s; %s)", version, runtime.GOOS, runtime.GOARCH)

	httpClient := &http.Client{
		Timeout: time.Second * 30,
	}

	client, _ := buildkite.NewOpts(
		buildkite.WithTokenAuth(apiToken),
		buildkite.WithUserAgent(userAgent),
		buildkite.WithHTTPClient(httpClient),
	)

	return &BuildkiteAPIClient{
		client: client,
	}
}

// NewBuildkiteAPI creates a new Buildkite API client using the provided go-buildkite client
func NewBuildkiteAPIExistingClient(client *buildkite.Client) *BuildkiteAPIClient {
	return &BuildkiteAPIClient{
		client: client,
	}
}

// GetJobLog fetches the log output for a specific job using go-buildkite
// org: organization slug
// pipeline: pipeline slug
// build: build number or UUID
// job: job ID
func (c *BuildkiteAPIClient) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	jobLog, _, err := c.client.Jobs.GetJobLog(ctx, org, pipeline, build, job)
	if err != nil {
		return nil, fmt.Errorf("failed to get job log: %w", err)
	}

	// Convert JobLog content to io.ReadCloser
	return io.NopCloser(strings.NewReader(jobLog.Content)), nil
}

// GetJobStatus gets the current status of a job with retry logic
func (c *BuildkiteAPIClient) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error) {
	return GetJobStatus(c.client, ctx, org, pipeline, build, job)
}

// ValidateAPIParams validates that all required API parameters are provided
func ValidateAPIParams(org, pipeline, build, job string) error {
	var missing []string

	if org == "" {
		missing = append(missing, "organization")
	}
	if pipeline == "" {
		missing = append(missing, "pipeline")
	}
	if build == "" {
		missing = append(missing, "build")
	}
	if job == "" {
		missing = append(missing, "job")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required API parameters: %s", strings.Join(missing, ", "))
	}

	return nil
}
