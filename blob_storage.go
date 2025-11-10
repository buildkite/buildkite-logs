package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"net/url"
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
	noTempDir := false
	if opts != nil {
		noTempDir = opts.NoTempDir
	}

	storageURL, err := GetDefaultStorageURL(storageURL, noTempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get default storage URL: %w", err)
	}

	// Open the bucket (supports file://, s3://, gcs://, etc.)
	bucket, err := blob.OpenBucket(ctx, storageURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open blob bucket %s: %w", storageURL, err)
	}

	return &BlobStorage{
		bucket: bucket,
	}, nil
}

// addNoTmpDirParam adds the no_tmp_dir=true parameter to a URL if not already present.
// Uses proper URL parsing to handle existing query parameters correctly.
func addNoTmpDirParam(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	q := u.Query()
	// Only add if not already present (handles any case/value variations)
	if q.Get("no_tmp_dir") == "" {
		q.Set("no_tmp_dir", "true")
		u.RawQuery = q.Encode()
	}

	return u.String(), nil
}

// GetDefaultStorageURL returns the default storage URL based on environment
//
// If noTempDir is true, the returned file:// URL will include the no_tmp_dir parameter,
// which causes gocloud.dev/blob/fileblob to create temporary files in the same directory
// as the final destination, avoiding cross-filesystem rename errors.
//
// This function applies the noTempDir setting to both user-provided and default URLs.
func GetDefaultStorageURL(storageURL string, noTempDir bool) (string, error) {
	var finalURL string

	if storageURL != "" {
		finalURL = storageURL
	} else {
		var dirPath string

		// Check if we're in a containerized environment (Docker/Kubernetes)
		if IsContainerizedEnvironment() {
			dirPath = fmt.Sprintf("%s/bklog", os.TempDir())
		} else {
			// Default to user's home directory for desktop usage
			homeDir, err := os.UserHomeDir()
			if err != nil {
				// Fallback to temp directory if home directory is unavailable
				dirPath = fmt.Sprintf("%s/bklog", os.TempDir())
			} else {
				dirPath = fmt.Sprintf("%s/.bklog", homeDir)
			}
		}

		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return "", fmt.Errorf("failed to create storage directory %s: %w", dirPath, err)
		}

		finalURL = fmt.Sprintf("file://%s", dirPath)
	}

	// Apply no_tmp_dir parameter to ALL file:// URLs if requested
	if noTempDir && strings.HasPrefix(finalURL, "file://") {
		var err error
		finalURL, err = addNoTmpDirParam(finalURL)
		if err != nil {
			return "", fmt.Errorf("failed to add no_tmp_dir parameter: %w", err)
		}
	}

	return finalURL, nil
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
	attrs, err := bs.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get blob attributes: %w", err)
	}

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
