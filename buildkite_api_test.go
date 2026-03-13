package buildkitelogs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildkite/go-buildkite/v4"
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

// TestGetJobStatus_RetriedJob verifies that GetJobStatus resolves the original
// job ID when it has been retried and replaced in the build's jobs array.
// See: https://github.com/buildkite/buildkite-mcp-server/issues/228
func TestGetJobStatus_RetriedJob(t *testing.T) {
	const (
		originalJobID    = "019ce437-f44f-4528-9d74-dfae306fed69"
		replacementJobID = "019ce438-3423-432e-a4f2-b0bfbffcc980"
	)

	// Simulate the Buildkite API response for build 76 with a retried job.
	// The original job is NOT in the jobs array — only the replacement is,
	// with retry_source.job_id pointing back to the original.
	buildResponse := buildkite.Build{
		Number: 76,
		Jobs: []buildkite.Job{
			{
				ID:    "019ce437-f433-4bc9-a728-8c4f4520e72e",
				State: "passed",
				Name:  "Build",
			},
			{
				ID:           replacementJobID,
				State:        "failed",
				Name:         "Test",
				RetriesCount: 1,
				RetrySource: &buildkite.JobRetrySource{
					JobID:     originalJobID,
					RetryType: "manual",
				},
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(buildResponse); err != nil {
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
		if status.State != JobStateFailed {
			t.Errorf("State = %q, want %q", status.State, JobStateFailed)
		}
	})

	t.Run("original retried job resolved via retry_source", func(t *testing.T) {
		status, err := apiClient.GetJobStatus(ctx, "myorg", "starter-pipeline", "76", originalJobID)
		if err != nil {
			t.Fatalf("GetJobStatus should resolve retried job, got: %v", err)
		}
		if status.ID != originalJobID {
			t.Errorf("ID = %q, want %q", status.ID, originalJobID)
		}
		if !status.IsTerminal {
			t.Error("retried job should be terminal")
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
