package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"
)

// BlobStorage provides an abstraction over blob storage backends
type BlobStorage struct {
	bucket *blob.Bucket
}

// BlobMetadata contains metadata for cached blobs
type BlobMetadata struct {
	JobID        string    `json:"job_id"`
	JobState     string    `json:"job_state"`
	IsTerminal   bool      `json:"is_terminal"`
	CachedAt     time.Time `json:"cached_at"`
	TTL          string    `json:"ttl"` // duration string like "30s"
	Organization string    `json:"organization"`
	Pipeline     string    `json:"pipeline"`
	Build        string    `json:"build"`
}

// NewBlobStorage creates a new blob storage instance from a storage URL
// Supports file:// URLs for local filesystem storage
func NewBlobStorage(ctx context.Context, storageURL string) (*BlobStorage, error) {
	storageURL, err := GetDefaultStorageURL(storageURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get default storage URL: %w", err)
	}

	// For other URLs (s3://, gcs://, etc.), use blob.OpenBucket
	bucket, err := blob.OpenBucket(ctx, storageURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob bucket %s: %w", storageURL, err)
	}

	return &BlobStorage{
		bucket: bucket,
	}, nil
}

// GetDefaultStorageURL returns the default storage URL based on environment
func GetDefaultStorageURL(storageURL string) (string, error) {
	// If a storage URL is provided, use it
	if storageURL != "" {
		return storageURL, nil
	}

	var dirPath string

	// Check if we're in a containerized environment (Docker/Kubernetes)
	if IsContainerizedEnvironment() {
		tempDir := os.TempDir()
		dirPath = fmt.Sprintf("%s/bklog", tempDir)
	} else {
		// Default to user's home directory for desktop usage
		homeDir, err := os.UserHomeDir()
		if err != nil {
			// Fallback to temp directory if home directory is unavailable
			tempDir := os.TempDir()
			dirPath = fmt.Sprintf("%s/bklog", tempDir)
		} else {
			dirPath = fmt.Sprintf("%s/.bklog", homeDir)
		}
	}

	// Ensure the directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create storage directory %s: %w", dirPath, err)
	}

	return fmt.Sprintf("file://%s", dirPath), nil
}

// IsContainerizedEnvironment detects if we're running in a container
func IsContainerizedEnvironment() bool {
	// Check for Docker environment indicators
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	// Check for Kubernetes environment indicators
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}

	// Check if running in common CI environments
	ciEnvVars := []string{
		"CI", "CONTINUOUS_INTEGRATION", "BUILDKITE", "GITHUB_ACTIONS",
		"GITLAB_CI", "JENKINS_URL", "TEAMCITY_VERSION",
	}
	for _, env := range ciEnvVars {
		if os.Getenv(env) != "" {
			return true
		}
	}

	return false
}

// GenerateBlobKey creates a consistent key for blob storage
func GenerateBlobKey(org, pipeline, build, job string) string {
	return fmt.Sprintf("%s-%s-%s-%s.parquet", org, pipeline, build, job)
}

// Exists checks if a blob exists in storage
func (bs *BlobStorage) Exists(ctx context.Context, key string) (bool, error) {
	return bs.bucket.Exists(ctx, key)
}

// WriteWithMetadata writes data to blob storage with metadata
func (bs *BlobStorage) WriteWithMetadata(ctx context.Context, key string, data []byte, metadata *BlobMetadata) error {
	opts := &blob.WriterOptions{}

	if metadata != nil {
		opts.Metadata = map[string]string{
			"job_id":       metadata.JobID,
			"job_state":    metadata.JobState,
			"is_terminal":  fmt.Sprintf("%t", metadata.IsTerminal),
			"cached_at":    metadata.CachedAt.Format(time.RFC3339),
			"ttl":          metadata.TTL,
			"organization": metadata.Organization,
			"pipeline":     metadata.Pipeline,
			"build":        metadata.Build,
		}
	}

	writer, err := bs.bucket.NewWriter(ctx, key, opts)
	if err != nil {
		return fmt.Errorf("failed to create blob writer: %w", err)
	}
	defer writer.Close()

	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write blob data: %w", err)
	}

	return nil
}

// ReadWithMetadata reads data from blob storage with metadata
func (bs *BlobStorage) ReadWithMetadata(ctx context.Context, key string) (*BlobMetadata, error) {
	// Get blob attributes for metadata
	attrs, err := bs.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob attributes: %w", err)
	}

	// Extract metadata
	var metadata *BlobMetadata
	if len(attrs.Metadata) > 0 {
		metadata = &BlobMetadata{}
		attrMap := attrs.Metadata

		metadata.JobID = attrMap["job_id"]
		metadata.JobState = attrMap["job_state"]
		metadata.IsTerminal = attrMap["is_terminal"] == "true"
		metadata.Organization = attrMap["organization"]
		metadata.Pipeline = attrMap["pipeline"]
		metadata.Build = attrMap["build"]
		metadata.TTL = attrMap["ttl"]

		if cachedAtStr := attrMap["cached_at"]; cachedAtStr != "" {
			if cachedAt, err := time.Parse(time.RFC3339, cachedAtStr); err == nil {
				metadata.CachedAt = cachedAt
			}
		}
	}

	return metadata, nil
}

// Reader returns an io.ReadCloser for streaming blob data from the specified key.
// The caller is responsible for closing the returned reader when done.
func (bs *BlobStorage) Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	return bs.bucket.NewReader(ctx, key, nil)
}

// GetModTime returns the modification time of a blob
func (bs *BlobStorage) GetModTime(ctx context.Context, key string) (time.Time, error) {
	attrs, err := bs.bucket.Attributes(ctx, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get blob attributes: %w", err)
	}
	return attrs.ModTime, nil
}

// Delete removes a blob from storage
func (bs *BlobStorage) Delete(ctx context.Context, key string) error {
	return bs.bucket.Delete(ctx, key)
}

// Close closes the blob storage connection
func (bs *BlobStorage) Close() error {
	return bs.bucket.Close()
}

// GetRuntimeInfo returns information about the current runtime environment
func GetRuntimeInfo() map[string]string {
	info := make(map[string]string)
	info["os"] = runtime.GOOS
	info["arch"] = runtime.GOARCH
	info["go_version"] = runtime.Version()
	info["num_cpu"] = fmt.Sprintf("%d", runtime.NumCPU())

	if hostname, err := os.Hostname(); err == nil {
		info["hostname"] = hostname
	}

	if wd, err := os.Getwd(); err == nil {
		info["working_dir"] = wd
	}

	// Add environment detection
	if IsContainerizedEnvironment() {
		info["environment"] = "container"
	} else {
		info["environment"] = "desktop"
	}

	return info
}
