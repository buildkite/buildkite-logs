package buildkitelogs

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildkite/go-buildkite/v4"
)

// MockBuildkiteAPI implements BuildkiteAPI for testing
type MockBuildkiteAPI struct {
	logContent string
	jobStatus  *JobStatus
}

func (m *MockBuildkiteAPI) GetJobLog(org, pipeline, build, job string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(m.logContent)), nil
}

func (m *MockBuildkiteAPI) GetJobStatus(org, pipeline, build, job string) (*JobStatus, error) {
	return m.jobStatus, nil
}

func TestParquetClient_NewParquetClient(t *testing.T) {
	// Create a mock buildkite client
	client, _ := buildkite.NewOpts()
	storageURL := "file://./test-cache"

	parquetClient, err := NewClient(client, storageURL)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer parquetClient.Close()

	if parquetClient == nil {
		t.Fatal("Expected ParquetClient to be created, got nil")
	}

	if parquetClient.storageURL != storageURL {
		t.Errorf("Expected storageURL %s, got %s", storageURL, parquetClient.storageURL)
	}
}

func TestParquetClient_NewParquetClientWithAPI(t *testing.T) {
	mockAPI := &MockBuildkiteAPI{
		logContent: "test log content",
		jobStatus: &JobStatus{
			ID:         "test-job",
			State:      JobStatePassed,
			IsTerminal: true,
		},
	}
	storageURL := "file://./test-cache"

	parquetClient, err := NewClientWithAPI(mockAPI, storageURL)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer parquetClient.Close()

	if parquetClient == nil {
		t.Fatal("Expected Client to be created, got nil")
	}

	if parquetClient.api != mockAPI {
		t.Error("Expected API to be set to mock API")
	}

	if parquetClient.storageURL != storageURL {
		t.Errorf("Expected storageURL %s, got %s", storageURL, parquetClient.storageURL)
	}
}

func TestParquetClient_DownloadAndCache(t *testing.T) {
	mockAPI := &MockBuildkiteAPI{
		logContent: "2024-01-01T00:00:00Z Test log entry\n",
		jobStatus: &JobStatus{
			ID:         "test-job",
			State:      JobStatePassed,
			IsTerminal: true,
		},
	}

	client, err := NewClientWithAPI(mockAPI, "file://./test-cache")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx := context.Background()

	// Test parameter validation
	_, err = client.DownloadAndCache(ctx, "", "pipeline", "build", "job", time.Minute, false)
	if err == nil {
		t.Error("Expected error for missing organization parameter")
	}

	// Note: Full integration test would require more setup for blob storage
	// This tests the parameter validation and basic structure
}

func TestParquetClient_NewReader(t *testing.T) {
	mockAPI := &MockBuildkiteAPI{
		logContent: "2024-01-01T00:00:00Z Test log entry\n",
		jobStatus: &JobStatus{
			ID:         "test-job",
			State:      JobStatePassed,
			IsTerminal: true,
		},
	}

	client, err := NewClientWithAPI(mockAPI, "file://./test-cache")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx := context.Background()

	// Test parameter validation
	_, err = client.NewReader(ctx, "", "pipeline", "build", "job", time.Minute, false)
	if err == nil {
		t.Error("Expected error for missing organization parameter")
	}

	// Note: Full integration test would require more setup for blob storage and file creation
	// This tests the parameter validation and basic structure
}
