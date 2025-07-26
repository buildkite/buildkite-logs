package buildkitelogs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// GetCacheDir returns the cache directory path, creating it if it doesn't exist
func GetCacheDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}

	cacheDir := filepath.Join(homeDir, ".bklog")

	// Create the cache directory if it doesn't exist
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create cache directory: %w", err)
	}

	return cacheDir, nil
}

// GenerateCacheFilename creates a cache filename based on org, pipeline, build, and job
func GenerateCacheFilename(org, pipeline, build, job string) string {
	return fmt.Sprintf("%s-%s-%s-%s.parquet", org, pipeline, build, job)
}

// GetCacheFilePath returns the full path to a cached parquet file
func GetCacheFilePath(org, pipeline, build, job string) (string, error) {
	cacheDir, err := GetCacheDir()
	if err != nil {
		return "", err
	}

	filename := GenerateCacheFilename(org, pipeline, build, job)
	return filepath.Join(cacheDir, filename), nil
}

// IsCacheFileExists checks if a cached parquet file exists
func IsCacheFileExists(org, pipeline, build, job string) (bool, string, error) {
	cacheFilePath, err := GetCacheFilePath(org, pipeline, build, job)
	if err != nil {
		return false, "", err
	}

	if _, err := os.Stat(cacheFilePath); os.IsNotExist(err) {
		return false, cacheFilePath, nil
	} else if err != nil {
		return false, cacheFilePath, fmt.Errorf("failed to check cache file: %w", err)
	}

	return true, cacheFilePath, nil
}

// DownloadAndCacheBlobStorage downloads logs using blob storage backend
func DownloadAndCacheBlobStorage(ctx context.Context, client BuildkiteAPI, org, pipeline, build, job, storageURL string, ttl time.Duration, forceRefresh bool) (string, error) {
	if ttl == 0 {
		ttl = 30 * time.Second // Default TTL from PRD
	}

	// Initialize blob storage
	blobStorage, err := NewBlobStorage(ctx, storageURL)
	if err != nil {
		return "", fmt.Errorf("failed to initialize blob storage: %w", err)
	}
	defer blobStorage.Close()

	blobKey := GenerateBlobKey(org, pipeline, build, job)

	// Check if blob already exists
	exists, err := blobStorage.Exists(blobKey)
	if err != nil {
		return "", fmt.Errorf("failed to check blob existence: %w", err)
	}

	// Get job status to determine caching strategy
	jobStatus, err := client.GetJobStatus(org, pipeline, build, job)
	if err != nil {
		return "", fmt.Errorf("failed to get job status: %w", err)
	}

	// Check if we should use existing cache
	if exists && !forceRefresh {
		_, metadata, err := blobStorage.ReadWithMetadata(blobKey)
		if err == nil && metadata != nil {
			// For terminal jobs, always use cache
			if metadata.IsTerminal {
				return createLocalCacheFile(ctx, blobStorage, blobKey, org, pipeline, build, job)
			}

			// For non-terminal jobs, check TTL
			timeElapsed := time.Since(metadata.CachedAt)
			if timeElapsed < ttl {
				return createLocalCacheFile(ctx, blobStorage, blobKey, org, pipeline, build, job)
			}
		}
	}

	// Download fresh logs from API
	logReader, err := client.GetJobLog(org, pipeline, build, job)
	if err != nil {
		return "", fmt.Errorf("failed to fetch logs from API: %w", err)
	}
	defer logReader.Close()

	// Parse logs and convert to parquet data
	parser := NewParser()
	var parquetData []byte

	// Create a temporary file for parquet export
	tempFile, err := os.CreateTemp("", "bklog-*.parquet")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()          // Close immediately so export can write to it
	defer os.Remove(tempPath) // Clean up temp file

	// Export to temporary parquet file
	countingSeq := func(yield func(*LogEntry, error) bool) {
		for entry, err := range parser.All(logReader) {
			if !yield(entry, err) {
				return
			}
		}
	}

	err = ExportSeq2ToParquetWithFilter(countingSeq, tempPath, nil)
	if err != nil {
		return "", fmt.Errorf("failed to export to parquet: %w", err)
	}

	// Read parquet data from temp file
	parquetData, err = os.ReadFile(tempPath)
	if err != nil {
		return "", fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Create metadata
	metadata := &BlobMetadata{
		JobID:        job,
		JobState:     string(jobStatus.State),
		IsTerminal:   jobStatus.IsTerminal,
		CachedAt:     time.Now(),
		TTL:          ttl.String(),
		Organization: org,
		Pipeline:     pipeline,
		Build:        build,
	}

	// Store in blob storage with metadata
	err = blobStorage.WriteWithMetadata(blobKey, parquetData, metadata)
	if err != nil {
		return "", fmt.Errorf("failed to write to blob storage: %w", err)
	}

	// Return local cache file path
	return createLocalCacheFile(ctx, blobStorage, blobKey, org, pipeline, build, job)
}

// createLocalCacheFile creates a local file from blob storage for compatibility
func createLocalCacheFile(ctx context.Context, blobStorage *BlobStorage, blobKey, org, pipeline, build, job string) (string, error) {
	// Get local cache file path for compatibility
	cacheFilePath, err := GetCacheFilePath(org, pipeline, build, job)
	if err != nil {
		return "", err
	}

	// Read from blob storage
	data, _, err := blobStorage.ReadWithMetadata(blobKey)
	if err != nil {
		return "", fmt.Errorf("failed to read from blob storage: %w", err)
	}

	// Write to local cache file
	err = os.WriteFile(cacheFilePath, data, 0600)
	if err != nil {
		return "", fmt.Errorf("failed to write local cache file: %w", err)
	}

	return cacheFilePath, nil
}
