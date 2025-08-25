package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// createLocalCacheFile creates a local file from blob storage for compatibility
func createLocalCacheFile(ctx context.Context, blobStorage *BlobStorage, blobKey string) (string, error) {
	cacheFilePath, err := os.CreateTemp("", "bklog-")
	if err != nil {
		return "", fmt.Errorf("failed to create local cache file: %w", err)
	}
	defer cacheFilePath.Close()

	// Read from blob storage
	reader, err := blobStorage.Reader(ctx, blobKey)
	if err != nil {
		return "", fmt.Errorf("failed to read from blob storage: %w", err)
	}
	defer reader.Close()

	// Write to local cache file
	_, err = io.Copy(cacheFilePath, reader)
	if err != nil {
		return "", fmt.Errorf("failed to write local cache file: %w", err)
	}

	return cacheFilePath.Name(), nil
}
