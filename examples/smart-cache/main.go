package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
	// Parse command line flags
	var (
		org      = flag.String("org", "", "Buildkite organization (required)")
		pipeline = flag.String("pipeline", "", "Pipeline slug (required)")
		build    = flag.String("build", "", "Build number (required)")
		job      = flag.String("job", "", "Job UUID (required)")
		version  = flag.String("version", "v1", "API version")
		token    = flag.String("token", "", "Buildkite API token (or use BUILDKITE_API_TOKEN env var)")
	)
	flag.Parse()

	// Get API token from flag or environment
	apiToken := *token
	if apiToken == "" {
		apiToken = os.Getenv("BUILDKITE_API_TOKEN")
	}
	if apiToken == "" {
		log.Fatal("Please provide API token via -token flag or BUILDKITE_API_TOKEN environment variable")
	}

	// Validate required parameters
	if *org == "" || *pipeline == "" || *build == "" || *job == "" {
		fmt.Println("Usage: go run main.go -org=<org> -pipeline=<pipeline> -build=<build> -job=<job>")
		fmt.Println("Example: go run main.go -org=my-org -pipeline=my-pipeline -build=123 -job=abc-123-def")
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Println("🚀 Buildkite Smart Caching Example")
	fmt.Println(strings.Repeat("=", 50))

	// Example 1: Default smart caching (30s TTL, auto storage)
	fmt.Println("\n📦 Example 1: Default smart caching")
	fmt.Println("- 30-second TTL for non-terminal jobs")
	fmt.Println("- Permanent cache for terminal jobs")
	fmt.Println("- Auto storage backend selection")

	start := time.Now()
	cacheFile1, err := buildkitelogs.DownloadAndCache(
		apiToken, *org, *pipeline, *build, *job, *version,
		"",             // empty storageURL = auto-detect (file://~/.bklog or file:///tmp/bklog)
		30*time.Second, // TTL for non-terminal jobs
		false,          // don't force refresh
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("✅ First download: %s (took %v)\n", cacheFile1, time.Since(start))
	}

	// Example 2: Immediate second call (should use cache)
	fmt.Println("\n⚡ Example 2: Immediate second call (cache hit)")
	start = time.Now()
	cacheFile2, err := buildkitelogs.DownloadAndCache(
		apiToken, *org, *pipeline, *build, *job, *version,
		"", 30*time.Second, false,
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("✅ Second download: %s (took %v)\n", cacheFile2, time.Since(start))
		fmt.Println("   → Should be much faster due to cache hit!")
	}

	// Example 3: Force refresh
	fmt.Println("\n🔄 Example 3: Force refresh (bypass cache)")
	start = time.Now()
	cacheFile3, err := buildkitelogs.DownloadAndCache(
		apiToken, *org, *pipeline, *build, *job, *version,
		"", 30*time.Second, true, // force refresh = true
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("✅ Force refresh: %s (took %v)\n", cacheFile3, time.Since(start))
		fmt.Println("   → Bypassed cache and downloaded fresh logs")
	}

	// Example 4: Custom storage backend (S3)
	fmt.Println("\n☁️  Example 4: Custom storage backend")
	fmt.Println("Storage URL examples:")
	fmt.Println("- file://~/.bklog (default desktop)")
	fmt.Println("- file:///tmp/bklog (default container)")
	fmt.Println("- s3://my-bucket/bklog-cache")
	fmt.Println("- gs://my-bucket/bklog-cache")

	// This would use S3 if AWS credentials are configured
	s3URL := "s3://my-buildkite-cache/logs"
	fmt.Printf("Storage URL for this example: %s\n", s3URL)

	start = time.Now()
	cacheFile4, err := buildkitelogs.DownloadAndCache(
		apiToken, *org, *pipeline, *build, *job, *version,
		s3URL,          // custom S3 storage
		60*time.Second, // longer TTL
		false,
	)
	if err != nil {
		log.Printf("S3 cache failed (expected if no AWS creds): %v", err)
		fmt.Println("   → Falling back to local storage for demo")

		// Fallback to local storage
		cacheFile4, err = buildkitelogs.DownloadAndCache(
			apiToken, *org, *pipeline, *build, *job, *version,
			"file://./cache-demo", // local demo cache
			60*time.Second,
			false,
		)
		if err != nil {
			log.Printf("Local cache fallback failed: %v", err)
		}
	}

	if err == nil && cacheFile4 != "" {
		fmt.Printf("✅ Custom storage: %s (took %v)\n", cacheFile4, time.Since(start))
	}

	// Example 5: Different TTL values
	fmt.Println("\n⏰ Example 5: Custom TTL values")
	ttlExamples := []time.Duration{
		5 * time.Second,  // Very short TTL
		2 * time.Minute,  // Medium TTL
		15 * time.Minute, // Long TTL
		0,                // Zero = default (30s)
	}

	for i, ttl := range ttlExamples {
		ttlDesc := ttl.String()
		if ttl == 0 {
			ttlDesc = "default (30s)"
		}

		fmt.Printf("  %d. TTL: %s\n", i+1, ttlDesc)

		start = time.Now()
		cacheFile, err := buildkitelogs.DownloadAndCache(
			apiToken, *org, *pipeline, *build, *job, *version,
			fmt.Sprintf("file://./cache-ttl-%d", i), // separate cache per TTL
			ttl,
			false,
		)
		if err != nil {
			log.Printf("     Cache failed: %v", err)
		} else {
			fmt.Printf("     ✅ Cached: %s (took %v)\n", cacheFile, time.Since(start))
		}
	}

	// Example 6: Demonstrate smart caching behavior
	fmt.Println("\n🧠 Example 6: Smart caching behavior")
	fmt.Println("This example would show different behavior based on job status:")
	fmt.Println("- Terminal jobs (finished/failed/canceled): cached permanently")
	fmt.Println("- Non-terminal jobs (running/pending): cached with TTL")

	// This would require actual job data to demonstrate properly
	fmt.Println("💡 Job status affects caching:")
	fmt.Println("   - finished/passed/failed/canceled/expired/timed_out/skipped/broken → permanent cache")
	fmt.Println("   - pending/waiting/running/etc → TTL-based cache (refreshes after TTL)")

	// Cleanup demo cache directories
	fmt.Println("\n🧹 Cleaning up demo cache directories...")
	for i := range ttlExamples {
		os.RemoveAll(fmt.Sprintf("./cache-ttl-%d", i))
	}
	os.RemoveAll("./cache-demo")

	fmt.Println("\n✅ Smart caching example completed!")
	fmt.Println("💡 Key features demonstrated:")
	fmt.Println("   - Automatic TTL management (30s default)")
	fmt.Println("   - Job status-aware caching (terminal vs non-terminal)")
	fmt.Println("   - Multiple storage backends (file, S3, GCS)")
	fmt.Println("   - Force refresh capability")
	fmt.Println("   - Metadata storage with job status and timestamps")
	fmt.Println("   - Environment-aware storage defaults")
}
