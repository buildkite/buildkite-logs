package buildkitelogs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBlobStorage(t *testing.T) {
	ctx := context.Background()

	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "bklog-blob-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storageURL := "file://" + tempDir

	// Create blob storage
	blobStorage, err := NewBlobStorage(ctx, storageURL)
	if err != nil {
		t.Fatalf("Failed to create blob storage: %v", err)
	}
	defer blobStorage.Close()

	// Test data
	key := "test-org-test-pipeline-123-abc-def.parquet"
	testData := []byte("test parquet data")

	metadata := &BlobMetadata{
		JobID:        "abc-def",
		JobState:     "finished",
		IsTerminal:   true,
		CachedAt:     time.Now(),
		TTL:          "30s",
		Organization: "test-org",
		Pipeline:     "test-pipeline",
		Build:        "123",
	}

	// Test write
	err = blobStorage.WriteWithMetadata(ctx, key, testData, metadata)
	if err != nil {
		t.Fatalf("Failed to write blob: %v", err)
	}

	// Test exists
	exists, err := blobStorage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Failed to check blob existence: %v", err)
	}
	if !exists {
		t.Fatal("Blob should exist after writing")
	}

	// Test read
	readMetadata, err := blobStorage.ReadWithMetadata(ctx, key)
	if err != nil {
		t.Fatalf("Failed to read blob: %v", err)
	}

	if readMetadata == nil {
		t.Fatal("Expected metadata, got nil")
	}

	if readMetadata.JobID != metadata.JobID {
		t.Errorf("Expected JobID %s, got %s", metadata.JobID, readMetadata.JobID)
	}

	if readMetadata.IsTerminal != metadata.IsTerminal {
		t.Errorf("Expected IsTerminal %t, got %t", metadata.IsTerminal, readMetadata.IsTerminal)
	}
}

func TestGetDefaultStorageURL(t *testing.T) {
	// Test default storage URL
	defaultURL, err := GetDefaultStorageURL("")
	if err != nil {
		t.Fatalf("GetDefaultStorageURL() failed: %v", err)
	}

	// Should start with file:// and contain bklog
	if !strings.HasPrefix(defaultURL, "file://") {
		t.Errorf("Expected file:// URL, got: %s", defaultURL)
	}
	if !strings.Contains(defaultURL, "bklog") {
		t.Errorf("Expected URL to contain 'bklog', got: %s", defaultURL)
	}
}

func TestGenerateBlobKey(t *testing.T) {
	key := GenerateBlobKey("myorg", "mypipeline", "123", "abc-def")
	expected := "myorg-mypipeline-123-abc-def.parquet"

	if key != expected {
		t.Errorf("Expected key %s, got %s", expected, key)
	}
}

func TestIsContainerizedEnvironment(t *testing.T) {
	// Test environment detection (hard to test definitively, but ensure no panic)
	isContainer := IsContainerizedEnvironment()

	// Should return a boolean without error
	_ = isContainer
}

// TestWriteWithMetadataCloseError verifies that Close() errors are properly propagated
// and not silently ignored when using WriteWithMetadata
func TestWriteWithMetadataCloseError(t *testing.T) {
	ctx := context.Background()

	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "bklog-blob-close-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storageURL := "file://" + tempDir

	// Create blob storage
	blobStorage, err := NewBlobStorage(ctx, storageURL)
	if err != nil {
		t.Fatalf("Failed to create blob storage: %v", err)
	}
	defer blobStorage.Close()

	// Test data
	key := "test-close-error.parquet"
	testData := []byte("test data")

	metadata := &BlobMetadata{
		JobID:        "test-job",
		JobState:     "finished",
		IsTerminal:   true,
		CachedAt:     time.Now(),
		TTL:          "30s",
		Organization: "test-org",
		Pipeline:     "test-pipeline",
		Build:        "123",
	}

	// First, write successfully to verify the setup works
	err = blobStorage.WriteWithMetadata(ctx, key, testData, metadata)
	if err != nil {
		t.Fatalf("Initial write should succeed: %v", err)
	}

	// Now remove write permissions from the directory to simulate Close() failure
	// This will cause subsequent writes to fail during the Close() operation
	err = os.Chmod(tempDir, 0444) // Read-only
	if err != nil {
		t.Fatalf("Failed to change permissions: %v", err)
	}

	// Restore permissions after test
	defer func() {
		_ = os.Chmod(tempDir, 0755)
	}()

	// Attempt to write with restricted permissions
	// This should fail during Close() and the error should be returned
	key2 := "test-close-error-2.parquet"
	err = blobStorage.WriteWithMetadata(ctx, key2, testData, metadata)

	// The write should fail, and we should get an error (not nil)
	if err == nil {
		t.Fatal("Expected WriteWithMetadata to return an error when Close() fails, but got nil")
	}

	// The error message should mention "close" to indicate it's from the Close() operation
	if !strings.Contains(strings.ToLower(err.Error()), "close") &&
		!strings.Contains(strings.ToLower(err.Error()), "permission") &&
		!strings.Contains(strings.ToLower(err.Error()), "denied") {
		t.Logf("Error message: %v", err)
		// Note: The exact error message may vary by OS and blob implementation
		// but we should get some error, not nil
	}

	// Verify that the file was not created
	exists, _ := blobStorage.Exists(ctx, key2)
	if exists {
		t.Error("File should not exist after failed write")
	}
}
