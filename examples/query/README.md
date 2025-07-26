# Query Example

This example demonstrates how to use the ParquetReader to stream and query Buildkite logs from Parquet files.

## Features Demonstrated

- **Streaming log entries** from Parquet files
- **Group-based filtering** and statistics
- **Memory-efficient iteration** with early termination
- **Direct file streaming** without loading entire file into memory

## Running the Example

```bash
cd examples/query
go run main.go
```

## What it does

1. **Stream all entries** and build group statistics
2. **Filter by group pattern** (e.g., "environment")  
3. **Direct file streaming** with memory efficiency
4. **Early termination** for large files

The example processes the test Parquet file and shows various querying patterns while maintaining constant memory usage regardless of file size.
