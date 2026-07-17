package buildkitelogs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildkite/go-buildkite/v5"
)

func TestParseBuildURL(t *testing.T) {
	tests := []struct {
		name         string
		buildURL     string
		wantPipeline string
		wantBuild    string
		expectError  bool
	}{
		{
			name:         "standard api url",
			buildURL:     "https://api.buildkite.com/v2/organizations/acme/pipelines/my-pipeline/builds/123",
			wantPipeline: "my-pipeline",
			wantBuild:    "123",
		},
		{
			name:         "build uuid",
			buildURL:     "https://api.buildkite.com/v2/organizations/acme/pipelines/deploy/builds/0190046e-e199-453b-a302-a21a4d649d31",
			wantPipeline: "deploy",
			wantBuild:    "0190046e-e199-453b-a302-a21a4d649d31",
		},
		{
			name:        "empty url",
			buildURL:    "",
			expectError: true,
		},
		{
			name:        "invalid url",
			buildURL:    "https://example.com/not-a-build-url",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, build, err := parseBuildURL(tt.buildURL)
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBuildURL() error = %v", err)
			}
			if pipeline != tt.wantPipeline {
				t.Errorf("pipeline = %q, want %q", pipeline, tt.wantPipeline)
			}
			if build != tt.wantBuild {
				t.Errorf("build = %q, want %q", build, tt.wantBuild)
			}
		})
	}
}

func TestValidateOrgJobParams(t *testing.T) {
	if err := ValidateOrgJobParams("org", "job"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateOrgJobParams("", "job"); err == nil {
		t.Fatal("expected error for missing org")
	}
	if err := ValidateOrgJobParams("org", ""); err == nil {
		t.Fatal("expected error for missing job")
	}
}

func TestOrgScopedJobAPI(t *testing.T) {
	const (
		org        = "buildkite"
		jobID      = "0190046e-e199-453b-a302-a21a4d649d31"
		pipeline   = "starter-pipeline"
		build      = "76"
		logContent = "\x1b_bk;t=1745322209921\x07Build failed: compile error\n"
	)

	jobResponse := buildkite.Job{
		ID:     jobID,
		State:  "failed",
		WebURL: "https://buildkite.com/buildkite/starter-pipeline/builds/76",
	}
	jobResponseWithURL := struct {
		buildkite.Job
		BuildURL string `json:"build_url"`
	}{
		Job:      jobResponse,
		BuildURL: "https://api.buildkite.com/v2/organizations/buildkite/pipelines/starter-pipeline/builds/76",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/log"):
			_ = json.NewEncoder(w).Encode(buildkite.JobLog{Content: logContent})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/jobs/"):
			_ = json.NewEncoder(w).Encode(jobResponseWithURL)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	bkClient, err := buildkite.NewOpts(
		buildkite.WithBaseURL(server.URL),
		buildkite.WithTokenAuth("test-token"),
	)
	if err != nil {
		t.Fatalf("NewOpts: %v", err)
	}

	apiClient := NewBuildkiteAPIExistingClient(bkClient)
	ctx := context.Background()

	t.Run("GetJobByOrg", func(t *testing.T) {
		job, err := apiClient.GetJobByOrg(ctx, org, jobID)
		if err != nil {
			t.Fatalf("GetJobByOrg: %v", err)
		}
		if job.ID != jobID {
			t.Errorf("ID = %q, want %q", job.ID, jobID)
		}
	})

	t.Run("GetJobLogByOrg", func(t *testing.T) {
		reader, err := apiClient.GetJobLogByOrg(ctx, org, jobID)
		if err != nil {
			t.Fatalf("GetJobLogByOrg: %v", err)
		}
		defer reader.Close()

		buf := make([]byte, len(logContent))
		n, err := reader.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(buf[:n]) != logContent {
			t.Errorf("log content = %q, want %q", string(buf[:n]), logContent)
		}
	})

	t.Run("ResolveJobLocation", func(t *testing.T) {
		location, err := ResolveJobLocation(ctx, apiClient, org, jobID)
		if err != nil {
			t.Fatalf("ResolveJobLocation: %v", err)
		}
		if location.Pipeline != pipeline {
			t.Errorf("Pipeline = %q, want %q", location.Pipeline, pipeline)
		}
		if location.Build != build {
			t.Errorf("Build = %q, want %q", location.Build, build)
		}
		if location.Job != jobID {
			t.Errorf("Job = %q, want %q", location.Job, jobID)
		}
	})

	t.Run("NewReaderByJobID", func(t *testing.T) {
		tempDir := t.TempDir()
		client, err := NewClientWithAPI(ctx, apiClient, "file://"+tempDir)
		if err != nil {
			t.Fatalf("NewClientWithAPI: %v", err)
		}
		defer client.Close()

		reader, err := client.NewReaderByJobID(ctx, org, jobID, 0, false)
		if err != nil {
			t.Fatalf("NewReaderByJobID: %v", err)
		}
		defer reader.Close()

		info, err := reader.GetFileInfo()
		if err != nil {
			t.Fatalf("GetFileInfo: %v", err)
		}
		if info.RowCount == 0 {
			t.Error("expected non-zero row count")
		}
	})
}

type customOrgScopedAPI struct {
	location JobLocation
	job      buildkite.Job
	log      string
}

func (a *customOrgScopedAPI) GetJobByOrg(ctx context.Context, org, jobID string) (buildkite.Job, error) {
	return a.job, nil
}

func (a *customOrgScopedAPI) GetJobLogByOrg(ctx context.Context, org, jobID string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(a.log)), nil
}

func (a *customOrgScopedAPI) GetJobLocationByOrg(ctx context.Context, org, jobID string) (JobLocation, error) {
	return a.location, nil
}

func (a *customOrgScopedAPI) GetJobLog(ctx context.Context, org, pipeline, build, job string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(a.log)), nil
}

func (a *customOrgScopedAPI) JobLogExists(ctx context.Context, org, pipeline, build, job string) (bool, error) {
	return true, nil
}

func (a *customOrgScopedAPI) GetJobStatus(ctx context.Context, org, pipeline, build, job string) (*JobStatus, error) {
	return jobStatusFromJob(a.job), nil
}

func TestNewReaderByJobIDSupportsCustomOrgScopedAPI(t *testing.T) {
	const (
		org        = "buildkite"
		jobID      = "0190046e-e199-453b-a302-a21a4d649d31"
		logContent = "\x1b_bk;t=1745322209921\x07custom api log line\n"
	)

	api := &customOrgScopedAPI{
		location: JobLocation{Org: org, Pipeline: "starter-pipeline", Build: "76", Job: jobID},
		job:      buildkite.Job{ID: jobID, State: "passed"},
		log:      logContent,
	}

	client, err := NewClientWithAPI(context.Background(), api, "file://"+t.TempDir())
	if err != nil {
		t.Fatalf("NewClientWithAPI: %v", err)
	}
	defer client.Close()

	reader, err := client.NewReaderByJobID(context.Background(), org, jobID, 0, false)
	if err != nil {
		t.Fatalf("NewReaderByJobID: %v", err)
	}
	defer reader.Close()

	info, err := reader.GetFileInfo()
	if err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}
	if info.RowCount == 0 {
		t.Error("expected non-zero row count")
	}
}

func TestGetJobLogByOrgUsesOrganizationEndpoint(t *testing.T) {
	const jobID = "0190046e-e199-453b-a302-a21a4d649d31"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/organizations/buildkite/jobs/"+jobID+"/log" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(buildkite.JobLog{Content: "log data"})
	}))
	defer server.Close()

	bkClient, err := buildkite.NewOpts(
		buildkite.WithBaseURL(server.URL),
		buildkite.WithTokenAuth("test-token"),
	)
	if err != nil {
		t.Fatalf("NewOpts: %v", err)
	}

	apiClient := NewBuildkiteAPIExistingClient(bkClient)
	reader, err := apiClient.GetJobLogByOrg(context.Background(), "buildkite", jobID)
	if err != nil {
		t.Fatalf("GetJobLogByOrg: %v", err)
	}
	defer reader.Close()
}
