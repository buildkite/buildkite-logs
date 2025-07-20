# ANSI Stripping Performance Comparison

## Why not use regex?

While regex is simpler to implement, it has significant performance and memory overhead for ANSI stripping in high-throughput scenarios like log processing.

## Benchmark Results

### Direct Performance Comparison (Typical Buildkite Log)
```
BenchmarkStripANSIComparison/Custom-10    526,899    6,619 ns/op    1,163 MB/s    
BenchmarkStripANSIComparison/Regex-10      26,035  139,110 ns/op       55 MB/s    
```

**Performance gain: ~21x faster** (1,163 MB/s vs 55 MB/s)

### Memory Allocation Comparison
```
BenchmarkStripANSIAllocs/Custom-10    656,391    5,302 ns/op    6,784 B/op     1 allocs/op
BenchmarkStripANSIAllocs/Regex-10      26,565  135,769 ns/op   14,723 B/op    12 allocs/op
```

**Memory efficiency:**
- **12x fewer allocations** (1 vs 12 allocs/op)
- **2.2x less memory** (6,784 vs 14,723 bytes/op)

### Real-world Impact

For processing 1 million log lines:
- **Custom parser**: ~6.6 seconds
- **Regex**: ~139 seconds (2.3 minutes)

The custom parser also has a fast-path optimization for strings without ANSI codes:
- **No ANSI content**: 91,530 MB/s (zero allocations)
- **With ANSI content**: 1,100-1,400 MB/s (single allocation)

## Implementation Trade-offs

| Aspect | Regex | Custom Parser |
|--------|-------|---------------|
| Code complexity | Low | Medium |
| Performance | Poor | Excellent |
| Memory usage | High | Low |
| Maintainability | High | Medium |
| Correctness | Good | Excellent |

## Conclusion

The custom parser is chosen because:

1. **20x performance improvement** is critical for log processing
2. **Significantly lower memory pressure** in high-throughput scenarios  
3. **Fast-path optimization** for strings without ANSI codes
4. **Single allocation** vs multiple regex allocations
5. **More precise** handling of edge cases (incomplete sequences, etc.)

The regex implementation is maintained for comparison and verification purposes.
