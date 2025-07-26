# Examples

This directory contains examples demonstrating different features of the buildkite-logs-parquet library.

## Available Examples

### [Query Example](./query/)
Demonstrates how to stream and query Buildkite logs from Parquet files:
- Memory-efficient streaming
- Group-based filtering
- Statistics collection
- Early termination patterns

### [Smart Cache Example](./smart-cache/)
Demonstrates Phase 3 smart caching with TTL and metadata management:
- TTL-based caching for non-terminal jobs
- Permanent caching for terminal jobs
- Multiple storage backends (file, S3, GCS)
- Force refresh and custom TTL values

## Running Examples

Each example is in its own directory with a `main.go` file:

```bash
# Query example
cd query && go run main.go

# Smart cache example  
cd smart-cache && go run main.go
```

See individual README files in each directory for specific setup instructions and requirements.
