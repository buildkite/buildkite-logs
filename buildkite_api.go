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

// BuildkiteAPIClient provides methods to interact with the Buildkite API
// Now wraps the official go-buildkite v4 client
type BuildkiteAPIClient struct {
	client    *buildkite.Client
	userAgent string
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
		client:    client,
		userAgent: userAgent,
	}
}

// GetJobLog fetches the log output for a specific job using go-buildkite
// org: organization slug
// pipeline: pipeline slug
// build: build number or UUID
// job: job ID
func (c *BuildkiteAPIClient) GetJobLog(org, pipeline, build, job string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobLog, _, err := c.client.Jobs.GetJobLog(ctx, org, pipeline, build, job)
	if err != nil {
		return nil, fmt.Errorf("failed to get job log: %w", err)
	}

	// Convert JobLog content to io.ReadCloser
	return io.NopCloser(strings.NewReader(jobLog.Content)), nil
}

// GetJobStatus gets the current status of a job with retry logic
func (c *BuildkiteAPIClient) GetJobStatus(org, pipeline, build, job string) (*JobStatus, error) {
	return GetJobStatus(c.client, org, pipeline, build, job)
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
