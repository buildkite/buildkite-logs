package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/buildkite/go-buildkite/v5"
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
	client       *buildkite.Client
	requireToken bool
	apiToken     string
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
		client:       client,
		requireToken: true,
		apiToken:     apiToken,
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
	if c.requireToken && c.apiToken == "" {
		return nil, fmt.Errorf("missing Buildkite API token")
	}

	u := fmt.Sprintf("v2/organizations/%s/pipelines/%s/builds/%s/jobs/%s/log", org, pipeline, build, job)
	req, err := c.client.NewRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create job log request: %w", err)
	}
	req.Header.Set("Accept", "text/plain")

	reader, writer := io.Pipe()
	go func() {
		_, err := c.client.Do(req, writer)
		if err != nil {
			err = &logDownloadError{err: err}
		}
		_ = writer.CloseWithError(err)
	}()

	return reader, nil
}

// GetJobStatus gets the current status of a job
func (c *BuildkiteAPIClient) GetJobStatus(ctx context.Context, org, pipeline, build, jobID string) (*JobStatus, error) {
	job, _, err := c.client.Jobs.GetJob(ctx, org, pipeline, build, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if job.ID != jobID {
		return nil, fmt.Errorf("job ID mismatch: got %q, want %q", job.ID, jobID)
	}

	return jobStatusFromJob(job), nil
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
