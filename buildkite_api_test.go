package buildkitelogs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildkite/go-buildkite/v5"
)

func TestValidateAPIParams(t *testing.T) {
	tests := []struct {
		name         string
		org          string
		pipeline     string
		build        string
		job          string
		expectError  bool
		errorMessage string
	}{
		{
			name:        "all_params_provided",
			org:         "myorg",
			pipeline:    "mypipeline",
			build:       "123",
			job:         "abc-def",
			expectError: false,
		},
		{
			name:         "missing_org",
			org:          "",
			pipeline:     "mypipeline",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: organization",
		},
		{
			name:         "missing_pipeline",
			org:          "myorg",
			pipeline:     "",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: pipeline",
		},
		{
			name:         "missing_build",
			org:          "myorg",
			pipeline:     "mypipeline",
			build:        "",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: build",
		},
		{
			name:         "missing_job",
			org:          "myorg",
			pipeline:     "mypipeline",
			build:        "123",
			job:          "",
			expectError:  true,
			errorMessage: "missing required API parameters: job",
		},
		{
			name:         "missing_multiple",
			org:          "",
			pipeline:     "",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: organization, pipeline",
		},
		{
			name:        "all_empty",
			org:         "",
			pipeline:    "",
			build:       "",
			job:         "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAPIParams(tt.org, tt.pipeline, tt.build, tt.job)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errorMessage != "" && err.Error() != tt.errorMessage {
					t.Errorf("Expected error message %q, got %q", tt.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestGetJobStatus_UsesJobEndpoint(t *testing.T) {
	const (
		originalJobID    = "019ce437-f44f-4528-9d74-dfae306fed69"
		replacementJobID = "019ce438-3423-432e-a4f2-b0bfbffcc980"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/v2/organizations/myorg/pipelines/starter-pipeline/builds/76/jobs/"
		if len(r.URL.Path) <= len(prefix) || r.URL.Path[:len(prefix)] != prefix {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}

		var job buildkite.Job
		switch r.URL.Path[len(prefix):] {
		case replacementJobID:
			job = buildkite.Job{ID: replacementJobID, State: "passed"}
		case originalJobID:
			job = buildkite.Job{ID: originalJobID, State: "failed"}
		default:
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(job); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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

	t.Run("replacement job found directly", func(t *testing.T) {
		status, err := apiClient.GetJobStatus(ctx, "myorg", "starter-pipeline", "76", replacementJobID)
		if err != nil {
			t.Fatalf("GetJobStatus: %v", err)
		}
		if status.ID != replacementJobID {
			t.Errorf("ID = %q, want %q", status.ID, replacementJobID)
		}
		if status.State != JobStatePassed {
			t.Errorf("State = %q, want %q", status.State, JobStatePassed)
		}
	})

	t.Run("original retried job fetched directly", func(t *testing.T) {
		status, err := apiClient.GetJobStatus(ctx, "myorg", "starter-pipeline", "76", originalJobID)
		if err != nil {
			t.Fatalf("GetJobStatus: %v", err)
		}
		if status.ID != originalJobID {
			t.Errorf("ID = %q, want %q", status.ID, originalJobID)
		}
		if !status.IsTerminal {
			t.Error("failed job should be terminal")
		}
	})

	t.Run("unknown job still returns error", func(t *testing.T) {
		_, err := apiClient.GetJobStatus(ctx, "myorg", "starter-pipeline", "76", "nonexistent-job-id")
		if err == nil {
			t.Fatal("expected error for unknown job ID")
		}
	})
}

func TestGetJobLog_NoToken(t *testing.T) {
	client := NewBuildkiteAPIClient("", "test")

	_, err := client.GetJobLog(context.TODO(), "org", "pipeline", "build", "job")
	if err == nil {
		t.Error("Expected error when API token is empty")
	}

	// The go-buildkite client will return a different error message for missing token
	// We just check that an error occurred
	if err == nil {
		t.Error("Expected an error when API token is empty")
	}
}

func TestJobLogExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
		}
		if r.URL.Path != "/v2/organizations/org/pipelines/pipeline/builds/123/jobs/job/log" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bkClient, err := buildkite.NewOpts(
		buildkite.WithBaseURL(server.URL),
		buildkite.WithTokenAuth("test-token"),
	)
	if err != nil {
		t.Fatalf("NewOpts: %v", err)
	}

	exists, err := NewBuildkiteAPIExistingClient(bkClient).JobLogExists(
		t.Context(), "org", "pipeline", "123", "job",
	)
	if err != nil {
		t.Fatalf("JobLogExists: %v", err)
	}
	if !exists {
		t.Fatal("JobLogExists = false, want true")
	}
}

func TestJobLogExists_NotFound(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	bkClient, err := buildkite.NewOpts(
		buildkite.WithBaseURL(server.URL),
		buildkite.WithTokenAuth("test-token"),
	)
	if err != nil {
		t.Fatalf("NewOpts: %v", err)
	}

	exists, err := NewBuildkiteAPIExistingClient(bkClient).JobLogExists(
		t.Context(), "org", "pipeline", "123", "missing-job",
	)
	if err != nil {
		t.Fatalf("JobLogExists: %v", err)
	}
	if exists {
		t.Fatal("JobLogExists = true, want false")
	}
}

func TestJobLogExists_NoToken(t *testing.T) {
	client := NewBuildkiteAPIClient("", "test")

	exists, err := client.JobLogExists(t.Context(), "org", "pipeline", "build", "job")
	if err == nil {
		t.Fatal("expected error when API token is empty")
	}
	if exists {
		t.Fatal("JobLogExists = true, want false")
	}
}

func TestGetJobLog_StreamsPlainText(t *testing.T) {
	const logContent = "\x1b_bk;t=1745322209921\x07first line\nsecond line\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/organizations/org/pipelines/pipeline/builds/123/jobs/job-1/log" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "text/plain" {
			t.Errorf("Accept = %q, want text/plain", got)
		}
		w.Header().Set("Content-Type", "text/plain")
		if _, err := io.WriteString(w, logContent); err != nil {
			t.Errorf("WriteString: %v", err)
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
	reader, err := apiClient.GetJobLog(t.Context(), "org", "pipeline", "123", "job-1")
	if err != nil {
		t.Fatalf("GetJobLog: %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != logContent {
		t.Fatalf("log content = %q, want %q", string(got), logContent)
	}
}
