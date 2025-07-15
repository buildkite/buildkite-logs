package buildkitelogs

import (
	"fmt"
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

// DownloadAndCache downloads logs from the Buildkite API and caches them as a parquet file
func DownloadAndCache(apiToken, org, pipeline, build, job, version string) (string, error) {
	// Check if cache file already exists
	exists, cacheFilePath, err := IsCacheFileExists(org, pipeline, build, job)
	if err != nil {
		return "", err
	}

	if exists {
		return cacheFilePath, nil
	}

	// Download logs from API
	client := NewBuildkiteAPIClient(apiToken, version)
	logReader, err := client.GetJobLog(org, pipeline, build, job)
	if err != nil {
		return "", fmt.Errorf("failed to fetch logs from API: %w", err)
	}
	defer logReader.Close()

	// Parse and export to parquet
	parser := NewParser()

	// Create filter function (no filtering for cache)
	countingSeq := func(yield func(*LogEntry, error) bool) {
		for entry, err := range parser.All(logReader) {
			if !yield(entry, err) {
				return
			}
		}
	}

	// Export to cache file
	err = ExportSeq2ToParquetWithFilter(countingSeq, cacheFilePath, nil)
	if err != nil {
		return "", fmt.Errorf("failed to export to cache: %w", err)
	}

	return cacheFilePath, nil
}
