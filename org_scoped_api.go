package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	buildkite "github.com/buildkite/go-buildkite/v5"
)

// OrgScopedJobAPI provides job access using only an organization slug and job UUID.
// This matches the organization-scoped REST endpoints documented at
// https://buildkite.com/docs/apis/rest-api/jobs. Implementations must also resolve
// pipeline/build identifiers so org-scoped readers can use compatible cache keys.
type OrgScopedJobAPI interface {
	GetJobByOrg(ctx context.Context, org, jobID string) (buildkite.Job, error)
	GetJobLogByOrg(ctx context.Context, org, jobID string) (io.ReadCloser, error)
	GetJobLocationByOrg(ctx context.Context, org, jobID string) (JobLocation, error)
}

type orgScopedLogAPI interface {
	OrgScopedJobAPI
	LogProvider
}

// JobLocation holds the identifiers needed for cache keys and pipeline-scoped API calls.
type JobLocation struct {
	Org      string
	Pipeline string
	Build    string
	Job      string
}

type jobByOrgResponse struct {
	buildkite.Job
	BuildURL string `json:"build_url"`
}

func organizationJobPath(organization, jobID, action string) string {
	base := fmt.Sprintf(
		"v2/organizations/%s/jobs/%s",
		url.PathEscape(organization),
		url.PathEscape(jobID),
	)
	if action == "" {
		return base
	}
	return base + "/" + action
}

func (c *BuildkiteAPIClient) getJobByOrgResponse(ctx context.Context, org, jobID string) (jobByOrgResponse, error) {
	req, err := c.client.NewRequest(ctx, "GET", organizationJobPath(org, jobID, ""), nil)
	if err != nil {
		return jobByOrgResponse{}, err
	}

	var job jobByOrgResponse
	if _, err := c.client.Do(req, &job); err != nil {
		return jobByOrgResponse{}, fmt.Errorf("failed to get job: %w", err)
	}

	return job, nil
}

// GetJobByOrg fetches a job using the organization-scoped REST endpoint.
func (c *BuildkiteAPIClient) GetJobByOrg(ctx context.Context, org, jobID string) (buildkite.Job, error) {
	job, err := c.getJobByOrgResponse(ctx, org, jobID)
	if err != nil {
		return buildkite.Job{}, err
	}
	return job.Job, nil
}

// GetJobLogByOrg fetches a job log using the organization-scoped REST endpoint.
func (c *BuildkiteAPIClient) GetJobLogByOrg(ctx context.Context, org, jobID string) (io.ReadCloser, error) {
	req, err := c.client.NewRequest(ctx, "GET", organizationJobPath(org, jobID, "log"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	var jobLog buildkite.JobLog
	if _, err := c.client.Do(req, &jobLog); err != nil {
		return nil, fmt.Errorf("failed to get job log: %w", err)
	}

	return io.NopCloser(strings.NewReader(jobLog.Content)), nil
}

// GetJobLocationByOrg fetches a job by organization and UUID, then resolves pipeline
// and build identifiers from the job's build_url for cache key compatibility.
func (c *BuildkiteAPIClient) GetJobLocationByOrg(ctx context.Context, org, jobID string) (JobLocation, error) {
	if err := ValidateOrgJobParams(org, jobID); err != nil {
		return JobLocation{}, err
	}

	jobResp, err := c.getJobByOrgResponse(ctx, org, jobID)
	if err != nil {
		return JobLocation{}, err
	}

	pipeline, build, err := parseBuildURL(jobResp.BuildURL)
	if err != nil {
		return JobLocation{}, fmt.Errorf("failed to resolve pipeline and build for job %s: %w", jobID, err)
	}

	return JobLocation{
		Org:      org,
		Pipeline: pipeline,
		Build:    build,
		Job:      jobID,
	}, nil
}

// ResolveJobLocation resolves the cache identifiers for an organization-scoped job.
func ResolveJobLocation(ctx context.Context, api OrgScopedJobAPI, org, jobID string) (JobLocation, error) {
	if err := ValidateOrgJobParams(org, jobID); err != nil {
		return JobLocation{}, err
	}
	return api.GetJobLocationByOrg(ctx, org, jobID)
}

// parseBuildURL extracts pipeline slug and build number/UUID from a Buildkite build_url.
// Example: https://api.buildkite.com/v2/organizations/acme/pipelines/my-pipeline/builds/123
func parseBuildURL(buildURL string) (pipeline, build string, err error) {
	if buildURL == "" {
		return "", "", fmt.Errorf("build_url is empty")
	}

	parsed, err := url.Parse(buildURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid build_url: %w", err)
	}

	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	// organizations/{org}/pipelines/{pipeline}/builds/{build}
	for i := 0; i+3 < len(segments); i++ {
		if segments[i] == "pipelines" && i+2 < len(segments) && segments[i+2] == "builds" {
			pipeline = segments[i+1]
			build = path.Base(strings.Join(segments[i+3:], "/"))
			if pipeline != "" && build != "" {
				return pipeline, build, nil
			}
		}
	}

	return "", "", fmt.Errorf("could not parse pipeline and build from build_url %q", buildURL)
}

// ValidateOrgJobParams validates organization and job UUID parameters.
func ValidateOrgJobParams(org, job string) error {
	var missing []string

	if org == "" {
		missing = append(missing, "organization")
	}
	if job == "" {
		missing = append(missing, "job")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required API parameters: %s", strings.Join(missing, ", "))
	}

	return nil
}

// jobStatusFromJob converts a Buildkite job model to JobStatus.
func jobStatusFromJob(job buildkite.Job) *JobStatus {
	state := JobState(job.State)
	status := &JobStatus{
		ID:         job.ID,
		State:      state,
		IsTerminal: IsTerminalState(state),
		WebURL:     job.WebURL,
	}

	if job.ExitStatus != nil {
		status.ExitStatus = job.ExitStatus
	}

	if job.FinishedAt != nil {
		finishedAt := job.FinishedAt.Time
		status.FinishedAt = &finishedAt
	}

	return status
}

// orgJobReaderAPI adapts OrgScopedJobAPI for the pipeline-scoped BuildkiteAPI interface
// while preserving org-only fetch semantics and resolved cache identifiers.
type orgJobReaderAPI struct {
	base     orgScopedLogAPI
	location JobLocation
}

func (a *orgJobReaderAPI) GetJobLog(ctx context.Context, _, _, _, _ string) (io.ReadCloser, error) {
	return a.base.GetJobLogByOrg(ctx, a.location.Org, a.location.Job)
}

func (a *orgJobReaderAPI) JobLogExists(ctx context.Context, _, _, _, _ string) (bool, error) {
	return a.base.JobLogExists(ctx, a.location.Org, a.location.Pipeline, a.location.Build, a.location.Job)
}

func (a *orgJobReaderAPI) GetJobStatus(ctx context.Context, _, _, _, _ string) (*JobStatus, error) {
	job, err := a.base.GetJobByOrg(ctx, a.location.Org, a.location.Job)
	if err != nil {
		return nil, err
	}
	return jobStatusFromJob(job), nil
}
