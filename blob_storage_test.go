package buildkitelogs

import (
	"context"
	"os"
	"path/filepath"
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
	err = blobStorage.WriteWithMetadata(key, testData, metadata)
	if err != nil {
		t.Fatalf("Failed to write blob: %v", err)
	}

	// Test exists
	exists, err := blobStorage.Exists(key)
	if err != nil {
		t.Fatalf("Failed to check blob existence: %v", err)
	}
	if !exists {
		t.Fatal("Blob should exist after writing")
	}

	// Test read
	readData, readMetadata, err := blobStorage.ReadWithMetadata(key)
	if err != nil {
		t.Fatalf("Failed to read blob: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Expected data %s, got %s", string(testData), string(readData))
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
	defaultURL := GetDefaultStorageURL()

	// Should return either desktop or container default
	if defaultURL != "file://~/.bklog" && defaultURL != "file:///tmp/bklog" {
		t.Errorf("Unexpected default storage URL: %s", defaultURL)
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

func TestBlobStorageFileURLParsing(t *testing.T) {
	ctx := context.Background()

	// Test different file URL formats
	testCases := []struct {
		name       string
		storageURL string
		shouldWork bool
	}{
		{
			name:       "absolute_path",
			storageURL: "file:///tmp/test-cache",
			shouldWork: true,
		},
		{
			name:       "relative_to_home",
			storageURL: "file://~/.test-cache",
			shouldWork: true,
		},
		{
			name:       "empty_url_uses_default",
			storageURL: "",
			shouldWork: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a unique temp directory for this test
			tempDir, err := os.MkdirTemp("", "bklog-url-test-*")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			// For file paths, replace with temp directory
			storageURL := tc.storageURL
			switch storageURL {
			case "file:///tmp/test-cache":
				storageURL = "file://" + filepath.Join(tempDir, "test-cache")
			case "file://~/.test-cache":
				storageURL = "file://" + filepath.Join(tempDir, ".test-cache")
			}

			blobStorage, err := NewBlobStorage(ctx, storageURL)

			if tc.shouldWork {
				if err != nil {
					t.Fatalf("Expected storage creation to succeed, got error: %v", err)
				}
				defer blobStorage.Close()
			} else if err == nil {
				blobStorage.Close()
				t.Fatal("Expected storage creation to fail, but it succeeded")
			}
		})
	}
}
