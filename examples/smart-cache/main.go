package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	buildkitelogs "github.com/buildkite/buildkite-logs"
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

	fmt.Println("Buildkite Smart Caching Example")
	fmt.Println(strings.Repeat("=", 50))

	// Example 1: Default smart caching using high-level Client (30s TTL, auto storage)
	fmt.Println("\nExample 1: Default smart caching with high-level Client")
	fmt.Println("- 30-second TTL for non-terminal jobs")
	fmt.Println("- Permanent cache for terminal jobs")
	fmt.Println("- Auto storage backend selection")

	ctx := context.Background()

	// Create high-level client
	buildkiteAPIClient := buildkitelogs.NewBuildkiteAPIClient(apiToken, *version)
	client, err := buildkitelogs.NewClientWithAPI(ctx, buildkiteAPIClient, "") // empty storageURL = auto-detect
	if err != nil {
		log.Fatal("Failed to create client:", err)
	}
	defer client.Close()

	start := time.Now()
	reader1, err := client.NewReader(
		ctx,
		*org, *pipeline, *build, *job,
		30*time.Second, // TTL for non-terminal jobs
		false,          // don't force refresh
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("First download took %v\n", time.Since(start))
		reader1.Close()
	}

	// Example 2: Immediate second call (should use cache)
	fmt.Println("\nExample 2: Immediate second call (cache hit)")
	start = time.Now()
	reader2, err := client.NewReader(
		ctx,
		*org, *pipeline, *build, *job,
		30*time.Second, false,
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("Second call took %v\n", time.Since(start))
		fmt.Println("   -> Should be much faster due to cache hit!")
		reader2.Close()
	}

	// Example 3: Force refresh
	fmt.Println("\nExample 3: Force refresh (bypass cache)")
	start = time.Now()
	reader3, err := client.NewReader(
		ctx,
		*org, *pipeline, *build, *job,
		30*time.Second, true, // force refresh = true
	)
	if err != nil {
		log.Printf("Cache operation failed: %v", err)
	} else {
		fmt.Printf("Force refresh took %v\n", time.Since(start))
		fmt.Println("   -> Bypassed cache and downloaded fresh logs")
		reader3.Close()
	}

	// Example 4: Custom storage backend (S3)
	fmt.Println("\nExample 4: Custom storage backend")
	fmt.Println("Storage URL examples:")
	fmt.Println("- file://~/.bklog (default desktop)")
	fmt.Println("- file:///tmp/bklog (default container)")
	fmt.Println("- s3://my-bucket/bklog-cache")
	fmt.Println("- gs://my-bucket/bklog-cache")

	// This would use S3 if AWS credentials are configured
	s3URL := "s3://my-buildkite-cache/logs"
	fmt.Printf("Storage URL for this example: %s\n", s3URL)

	// Create client with custom storage URL
	s3Client, err := buildkitelogs.NewClientWithAPI(ctx, buildkiteAPIClient, s3URL)
	if err != nil {
		log.Printf("Failed to create S3 client: %v", err)
		fmt.Println("   -> Falling back to local storage for demo")
	} else {
		defer s3Client.Close()
		start = time.Now()
		reader4, err := s3Client.NewReader(
			ctx,
			*org, *pipeline, *build, *job,
			60*time.Second, // longer TTL
			false,
		)
		if err != nil {
			log.Printf("S3 cache failed (expected if no AWS creds): %v", err)
			fmt.Println("   -> Falling back to local storage for demo")
		} else {
			fmt.Printf("Custom storage took %v\n", time.Since(start))
			reader4.Close()
		}
	}

	// Example 5: Different TTL values
	fmt.Println("\nExample 5: Custom TTL values")
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

		// Create client with separate cache directory for each TTL
		ttlClient, err := buildkitelogs.NewClientWithAPI(ctx, buildkiteAPIClient, fmt.Sprintf("file://./cache-ttl-%d", i))
		if err != nil {
			log.Printf("     Failed to create TTL client: %v", err)
			continue
		}
		defer ttlClient.Close()
		start = time.Now()
		reader, err := ttlClient.NewReader(
			ctx,
			*org, *pipeline, *build, *job,
			ttl,
			false,
		)
		if err != nil {
			log.Printf("     Cache failed: %v", err)
		} else {
			fmt.Printf("     Cached in %v\n", time.Since(start))
			reader.Close()
		}
	}

	// Example 6: Demonstrate smart caching behavior
	fmt.Println("\nSmart caching behavior:")
	fmt.Println("- Terminal jobs (finished/failed/canceled): cached permanently")
	fmt.Println("- Non-terminal jobs (running/pending): cached with TTL")

	// Cleanup demo cache directories
	fmt.Println("\nCleaning up demo cache directories...")
	for i := range ttlExamples {
		os.RemoveAll(fmt.Sprintf("./cache-ttl-%d", i))
	}
	os.RemoveAll("./cache-demo")

	fmt.Println("\nSmart caching example completed!")
}
