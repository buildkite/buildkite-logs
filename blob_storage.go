package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
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

// BlobStorageOptions contains configuration options for blob storage
type BlobStorageOptions struct {
	// NoTempDir controls whether to use the no_tmp_dir URL parameter for file:// URLs.
	// When true, temporary files are created in the same directory as the final destination,
	// avoiding cross-filesystem rename errors. This may result in stranded .tmp files if
	// the process crashes before cleanup runs.
	//
	// When false (default), temporary files are created in os.TempDir(), which may cause
	// "invalid cross-device link" errors if the temp directory is on a different filesystem
	// than the storage directory.
	NoTempDir bool
}

// NewBlobStorage creates a new blob storage instance from a storage URL
// Supports file:// URLs for local filesystem storage
//
// The opts parameter allows configuring blob storage behavior. Pass nil to use default options.
func NewBlobStorage(ctx context.Context, storageURL string, opts *BlobStorageOptions) (*BlobStorage, error) {
	// Extract options, using defaults if nil
	noTempDir := false
	if opts != nil {
		noTempDir = opts.NoTempDir
	}

	// Get or build the storage URL
	storageURL, err := GetDefaultStorageURL(storageURL, noTempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get default storage URL: %w", err)
	}

	// If user provided a file:// URL and NoTempDir is requested, ensure the parameter is added
	if noTempDir && len(storageURL) > 7 && storageURL[:7] == "file://" {
		// Check if no_tmp_dir parameter is already present
		if !containsNoTmpDir(storageURL) {
			// Add the parameter
			if containsQueryString(storageURL) {
				storageURL += "&no_tmp_dir=true"
			} else {
				storageURL += "?no_tmp_dir=true"
			}
		}
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

// containsNoTmpDir checks if a URL already has the no_tmp_dir parameter
func containsNoTmpDir(url string) bool {
	return strings.Contains(url, "no_tmp_dir=true")
}

// containsQueryString checks if a URL has a query string
func containsQueryString(url string) bool {
	return strings.Contains(url, "?")
}

// GetDefaultStorageURL returns the default storage URL based on environment
//
// If noTempDir is true, the returned file:// URL will include the no_tmp_dir parameter,
// which causes gocloud.dev/blob/fileblob to create temporary files in the same directory
// as the final destination, avoiding cross-filesystem rename errors.
func GetDefaultStorageURL(storageURL string, noTempDir bool) (string, error) {
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

	// Build the URL
	url := fmt.Sprintf("file://%s", dirPath)

	// Add no_tmp_dir parameter if requested
	if noTempDir {
		url += "?no_tmp_dir=true"
	}

	return url, nil
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

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write blob data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close blob writer: %w", err)
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
