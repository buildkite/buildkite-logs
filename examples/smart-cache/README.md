# Smart Cache Example

This example demonstrates smart caching features using the high-level `Client` API with TTL and metadata management.

## Features Demonstrated

- **High-level Client API** for simplified caching operations
- **30-second TTL** for non-terminal jobs with automatic refresh
- **Permanent caching** for terminal jobs (finished/failed/canceled) 
- **Force refresh** capability bypassing cache
- **Multiple storage backends** (file://, s3://, gs://)
- **Environment-aware defaults** (~/.bklog desktop, /tmp/bklog container)
- **Custom TTL values** and metadata management

## Prerequisites

Set your Buildkite API token:
```bash
export BUILDKITE_API_TOKEN=your_token_here
```

## Running the Example

Provide your Buildkite job details via command line flags:

```bash
cd examples/smart-cache

# Basic usage with environment token
go run main.go -org=my-org -pipeline=my-pipeline -build=123 -job=abc-123-def

# With custom token and version
go run main.go \
  -org=my-org \
  -pipeline=my-pipeline \
  -build=123 \
  -job=abc-123-def \
  -token=bkua_xxx \
  -version=v2
```

## Command Line Flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-org` | ✅ | - | Buildkite organization slug |
| `-pipeline` | ✅ | - | Pipeline slug |
| `-build` | ✅ | - | Build number |
| `-job` | ✅ | - | Job UUID |
| `-token` | - | `$BUILDKITE_API_TOKEN` | API token (or use env var) |
| `-version` | - | `v1` | API version |

## What it demonstrates

1. **Default smart caching** with 30s TTL
2. **Cache hits** on immediate subsequent calls
3. **Force refresh** bypassing cache
4. **Custom storage backends** (S3, GCS, local)
5. **Different TTL values** and their effects
6. **Job status-aware caching** behavior

The example shows how the high-level `Client` API handles terminal jobs by caching them permanently while non-terminal jobs refresh based on TTL, with metadata stored in blob attributes. Each example creates separate clients with different storage URLs to demonstrate various caching scenarios.
