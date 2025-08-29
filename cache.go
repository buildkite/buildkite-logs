package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"os"
)

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
