package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	buildkitelogs "github.com/buildkite/buildkite-logs"
)

func handleQueryCommand() {
	var config QueryConfig

	queryFlags := flag.NewFlagSet("query", flag.ExitOnError)
	queryFlags.StringVar(&config.ParquetFile, "file", "", "Path to Parquet log file (use this OR API parameters)")
	queryFlags.StringVar(&config.Operation, "op", "list-groups", "Query operation: list-groups, by-group, info, tail, seek, dump, search")
	queryFlags.StringVar(&config.GroupName, "group", "", "Group name to filter by (for by-group operation)")
	queryFlags.StringVar(&config.Format, "format", "text", "Output format: text, json")
	queryFlags.BoolVar(&config.ShowStats, "stats", true, "Show query statistics")
	queryFlags.IntVar(&config.LimitEntries, "limit", 0, "Limit number of entries returned (0 = no limit, enables early termination)")
	queryFlags.IntVar(&config.TailLines, "tail", 10, "Number of lines to show from end (for tail operation)")
	queryFlags.Int64Var(&config.SeekToRow, "seek", 0, "Row number to seek to (0-based, for seek operation)")
	queryFlags.BoolVar(&config.RawOutput, "raw", false, "Output raw log content without timestamps, groups, or other prefixes")
	// Search operation parameters
	queryFlags.StringVar(&config.SearchPattern, "pattern", "", "Regex pattern to search for (for search operation)")
	queryFlags.IntVar(&config.AfterContext, "A", 0, "Show NUM lines after each match")
	queryFlags.IntVar(&config.BeforeContext, "B", 0, "Show NUM lines before each match")
	queryFlags.IntVar(&config.Context, "C", 0, "Show NUM lines before and after each match")
	queryFlags.BoolVar(&config.CaseSensitive, "case-sensitive", false, "Case-sensitive search")
	queryFlags.BoolVar(&config.InvertMatch, "invert-match", false, "Show non-matching lines")
	queryFlags.BoolVar(&config.Reverse, "reverse", false, "Search backwards from end/seek position")
	queryFlags.Int64Var(&config.SearchSeek, "search-seek", 0, "Start search from this row (useful with --reverse)")
	// Buildkite API parameters
	// ANSI processing flag
	queryFlags.BoolVar(&config.StripANSI, "strip-ansi", false, "Strip ANSI escape codes from log content")
	// Buildkite API parameters
	queryFlags.StringVar(&config.Organization, "org", "", "Buildkite organization slug (for API)")
	queryFlags.StringVar(&config.Pipeline, "pipeline", "", "Buildkite pipeline slug (for API)")
	queryFlags.StringVar(&config.Build, "build", "", "Buildkite build number or UUID (for API)")
	queryFlags.StringVar(&config.Job, "job", "", "Buildkite job ID (for API)")
	// Smart caching parameters
	queryFlags.DurationVar(&config.CacheTTL, "cache-ttl", 30*time.Second, "Cache TTL for non-terminal jobs")
	queryFlags.BoolVar(&config.ForceRefresh, "cache-force-refresh", false, "Force refresh cached entry")
	queryFlags.StringVar(&config.CacheURL, "cache-url", "", "Cache storage URL (file://path, s3://bucket, etc)")

	queryFlags.Usage = func() {
		fmt.Printf("Usage: %s query [options]\n\n", os.Args[0])
		fmt.Println("Query Parquet log files from local files or Buildkite API.")
		fmt.Println("\nYou must provide either:")
		fmt.Println("  -file <path>     Local parquet file")
		fmt.Println("  OR API params:   -org -pipeline -build -job")
		fmt.Println("\nFor API usage, set BUILDKITE_API_TOKEN environment variable.")
		fmt.Println("API logs are automatically downloaded and cached using the high-level client.")
		fmt.Println("Smart caching: Terminal jobs are cached permanently, non-terminal jobs use TTL.")
		fmt.Println("\nOptions:")
		queryFlags.PrintDefaults()
		fmt.Println("\nOperations:")
		fmt.Println("  list-groups    List all groups with statistics")
		fmt.Println("  by-group       Show entries for a specific group")
		fmt.Println("  search         Search entries using regex pattern with context")
		fmt.Println("  info           Show file metadata (row count, file size, etc.)")
		fmt.Println("  tail           Show last N entries from the file")
		fmt.Println("  seek           Start reading from a specific row number")
		fmt.Println("  dump           Output all entries from the file")
		fmt.Println("\nExamples:")
		fmt.Printf("  # Local file:\n")
		fmt.Printf("  %s query -file logs.parquet -op list-groups\n", os.Args[0])

		fmt.Printf("  %s query -file logs.parquet -op by-group -group \"Running tests\"\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op search -pattern \"error|failed\" -C 3\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op search -pattern \"test.*failed\" --reverse -C 2\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op search -pattern \"setup\" --reverse --search-seek 1000\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op info\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op tail -tail 20\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op seek -seek 1000 -limit 50\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op dump -limit 100\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op dump -raw\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op dump -strip-ansi\n", os.Args[0])
		fmt.Printf("\n  # API:\n")
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op list-groups\n", os.Args[0])
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op by-group -group \"Running tests\"\n", os.Args[0])
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op info\n", os.Args[0])
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op info -cache-force-refresh\n", os.Args[0])
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op list-groups -cache-ttl=60s\n", os.Args[0])
		fmt.Printf("  %s query -org myorg -pipeline mypipe -build 123 -job abc-def -op info -cache-url=file:///tmp/cache\n", os.Args[0])
	}

	if err := queryFlags.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	// Validate that either file or API parameters are provided
	hasFile := config.ParquetFile != ""
	hasAPIParams := config.Organization != "" || config.Pipeline != "" || config.Build != "" || config.Job != ""

	if !hasFile && !hasAPIParams {
		fmt.Fprintf(os.Stderr, "Error: Must provide either -file or API parameters (-org, -pipeline, -build, -job)\n\n")
		queryFlags.Usage()
		os.Exit(1)
	}

	if hasFile && hasAPIParams {
		fmt.Fprintf(os.Stderr, "Error: Cannot use both -file and API parameters simultaneously\n\n")
		queryFlags.Usage()
		os.Exit(1)
	}

	// If using API, validate all required parameters are present
	if hasAPIParams {
		if err := buildkitelogs.ValidateAPIParams(config.Organization, config.Pipeline, config.Build, config.Job); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
			queryFlags.Usage()
			os.Exit(1)
		}
	}

	if err := runQuery(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// formatLogEntries formats a slice of log entries consistently across all operations
func formatLogEntries(entries []buildkitelogs.ParquetLogEntry, config *QueryConfig) {
	if config.RawOutput {
		// Raw mode: just print content to stdout
		for _, entry := range entries {
			content := entry.CleanContent(config.StripANSI)
			fmt.Println(content)
		}
	} else {
		// Formatted mode: print with timestamps and markers to stdout
		for _, entry := range entries {
			timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))

			var markers []string
			if entry.IsGroup() {
				markers = append(markers, "GRP")
			}

			markerStr := ""
			if len(markers) > 0 {
				markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
			}

			content := entry.CleanContent(config.StripANSI)
			group := entry.CleanGroup(config.StripANSI)

			// For group entries where group name == content, don't show duplicate
			if group != "" && group != content {
				fmt.Printf("[%s] [%s]%s %s\n",
					timestamp.Format("2006-01-02 15:04:05.000"),
					group,
					markerStr,
					content)
			} else {
				fmt.Printf("[%s]%s %s\n",
					timestamp.Format("2006-01-02 15:04:05.000"),
					markerStr,
					content)
			}
		}
	}
}

// formatSearchResults formats search results consistently
func formatSearchResults(results []buildkitelogs.SearchResult, config *QueryConfig) {
	if config.RawOutput {
		// Raw mode: just print content to stdout
		for _, result := range results {
			// Print before context
			for _, entry := range result.BeforeContext {
				content := entry.CleanContent(config.StripANSI)
				fmt.Println(content)
			}
			// Print match line
			content := result.Match.CleanContent(config.StripANSI)
			fmt.Println(content)
			// Print after context
			for _, entry := range result.AfterContext {
				content := entry.CleanContent(config.StripANSI)
				fmt.Println(content)
			}
		}
	} else {
		// Formatted mode: print with timestamps and context separators
		for i, result := range results {
			if i > 0 {
				fmt.Println("--")
			}

			// Print before context
			for _, entry := range result.BeforeContext {
				timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
				content := entry.CleanContent(config.StripANSI)
				group := entry.CleanGroup(config.StripANSI)
				if group != "" {
					fmt.Printf("[%s] [%s] %s\n",
						timestamp.Format("2006-01-02 15:04:05.000"),
						group,
						content)
				} else {
					fmt.Printf("[%s] %s\n",
						timestamp.Format("2006-01-02 15:04:05.000"),
						content)
				}
			}

			// Print match line (highlighted)
			timestamp := time.Unix(0, result.Match.Timestamp*int64(time.Millisecond))
			content := result.Match.CleanContent(config.StripANSI)
			group := result.Match.CleanGroup(config.StripANSI)
			if group != "" {
				fmt.Printf("[%s] [%s] MATCH: %s\n",
					timestamp.Format("2006-01-02 15:04:05.000"),
					group,
					content)
			} else {
				fmt.Printf("[%s] MATCH: %s\n",
					timestamp.Format("2006-01-02 15:04:05.000"),
					content)
			}

			// Print after context
			for _, entry := range result.AfterContext {
				timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
				content := entry.CleanContent(config.StripANSI)
				group := entry.CleanGroup(config.StripANSI)
				if group != "" {
					fmt.Printf("[%s] [%s] %s\n",
						timestamp.Format("2006-01-02 15:04:05.000"),
						group,
						content)
				} else {
					fmt.Printf("[%s] %s\n",
						timestamp.Format("2006-01-02 15:04:05.000"),
						content)
				}
			}
		}
	}
}

// QueryConfig holds configuration for CLI query operations
type QueryConfig struct {
	ParquetFile  string
	Operation    string // "list-groups", "by-group", "info", "tail"
	GroupName    string
	Format       string // "text", "json"
	ShowStats    bool
	LimitEntries int   // Limit output entries (0 = no limit)
	TailLines    int   // Number of lines to show from end (for tail operation)
	SeekToRow    int64 // Row number to seek to (0-based)
	RawOutput    bool  // Output raw log content without timestamps, groups, or other prefixes
	// Search operation parameters
	SearchPattern string // Regex pattern to search for
	AfterContext  int    // Lines to show after match
	BeforeContext int    // Lines to show before match
	Context       int    // Lines to show before and after match
	CaseSensitive bool   // Case-sensitive search
	InvertMatch   bool   // Show non-matching lines
	Reverse       bool   // Search backwards from end/seek position
	SearchSeek    int64  // Start search from this row (useful with Reverse)
	// ANSI processing
	StripANSI bool // Strip ANSI escape codes from log content
	// Buildkite API parameters
	Organization string
	Pipeline     string
	Build        string
	Job          string
	// Smart caching parameters
	CacheTTL     time.Duration // Cache TTL for non-terminal jobs
	ForceRefresh bool          // Force refresh cached entry
	CacheURL     string        // Cache storage URL
}

// runQuery executes a query using streaming iterators
func runQuery(config *QueryConfig) error {
	// Resolve the parquet file path
	parquetFile, err := resolveParquetFilePath(config)
	if err != nil {
		return err
	}

	reader := buildkitelogs.NewParquetReader(parquetFile)
	return runStreamingQuery(reader, config)
}

// resolveParquetFilePath determines the parquet file path to use
func resolveParquetFilePath(config *QueryConfig) (string, error) {
	// If file path is provided directly, use it
	if config.ParquetFile != "" {
		return config.ParquetFile, nil
	}

	// If API parameters are provided, download and cache using high-level client
	if config.Organization != "" && config.Pipeline != "" && config.Build != "" && config.Job != "" {
		apiToken := os.Getenv("BUILDKITE_API_TOKEN")
		if apiToken == "" {
			return "", fmt.Errorf("BUILDKITE_API_TOKEN environment variable is required for API access")
		}

		// Create buildkite client and high-level client
		buildkiteClient := buildkitelogs.NewBuildkiteAPIClient(apiToken, version)
		client, err := buildkitelogs.NewClientWithAPI(buildkiteClient, config.CacheURL)
		if err != nil {
			return "", fmt.Errorf("failed to create client: %w", err)
		}
		defer client.Close()
		ctx := context.Background()

		cacheFilePath, err := client.DownloadAndCache(ctx, config.Organization, config.Pipeline, config.Build, config.Job, config.CacheTTL, config.ForceRefresh)
		if err != nil {
			return "", fmt.Errorf("failed to download and cache logs: %w", err)
		}

		return cacheFilePath, nil
	}

	return "", fmt.Errorf("either -file or API parameters must be provided")
}

// runStreamingQuery executes streaming queries for memory efficiency
func runStreamingQuery(reader *buildkitelogs.ParquetReader, config *QueryConfig) error {
	start := time.Now()

	switch config.Operation {
	case "list-groups":
		return streamListGroups(reader, config, start)

	case "info":
		return showFileInfo(reader, config)
	case "by-group":
		if config.GroupName == "" {
			return fmt.Errorf("group pattern is required for by-group operation")
		}
		return streamByGroup(reader, config, start)
	case "search":
		if config.SearchPattern == "" {
			return fmt.Errorf("pattern is required for search operation")
		}
		return streamSearch(reader, config, start)
	case "tail":
		return tailFile(reader, config, start)
	case "seek":
		return seekToRow(reader, config, start)
	case "dump":
		return streamDump(reader, config, start)
	default:
		return fmt.Errorf("unknown operation: %s", config.Operation)
	}
}

// streamListGroups handles list-groups operation using streaming
func streamListGroups(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	// Use streaming iterator to build group statistics
	groupMap := make(map[string]*buildkitelogs.GroupInfo)
	totalEntries := 0

	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		totalEntries++

		groupName := entry.Group
		if groupName == "" {
			groupName = "<no group>"
		}

		info, exists := groupMap[groupName]
		if !exists {
			entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
			info = &buildkitelogs.GroupInfo{
				Name:      groupName,
				FirstSeen: entryTime,
				LastSeen:  entryTime,
			}
			groupMap[groupName] = info
		}

		info.EntryCount++

		entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
		if entryTime.Before(info.FirstSeen) {
			info.FirstSeen = entryTime
		}
		if entryTime.After(info.LastSeen) {
			info.LastSeen = entryTime
		}

	}

	// Convert to slice and sort
	groups := make([]buildkitelogs.GroupInfo, 0, len(groupMap))
	for _, info := range groupMap {
		groups = append(groups, *info)
	}

	// Sort by first seen time (simple sorting)
	for i := 0; i < len(groups)-1; i++ {
		for j := i + 1; j < len(groups); j++ {
			if groups[j].FirstSeen.Before(groups[i].FirstSeen) {
				groups[i], groups[j] = groups[j], groups[i]
			}
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatStreamingGroupsResult(groups, totalEntries, queryTime, config)
}

// streamSearch handles search operation using streaming with regex pattern matching and context lines
func streamSearch(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	// Create search options
	options := buildkitelogs.SearchOptions{
		Pattern:       config.SearchPattern,
		CaseSensitive: config.CaseSensitive,
		InvertMatch:   config.InvertMatch,
		BeforeContext: config.BeforeContext,
		AfterContext:  config.AfterContext,
		Context:       config.Context,
		Reverse:       config.Reverse,
		SeekStart:     config.SearchSeek,
	}

	var results []buildkitelogs.SearchResult
	matchesFound := 0

	for result, err := range reader.SearchEntriesIter(options) {
		if err != nil {
			return fmt.Errorf("error during search: %w", err)
		}

		matchesFound++
		results = append(results, result)

		// Apply limit if specified
		if config.LimitEntries > 0 && matchesFound >= config.LimitEntries {
			break
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatSearchResultsLibrary(results, matchesFound, queryTime, config)
}

// streamByGroup handles by-group operation using streaming with optional limiting
func streamByGroup(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	var entries []buildkitelogs.ParquetLogEntry
	totalEntries := 0
	matchedEntries := 0

	for entry, err := range reader.FilterByGroupIter(config.GroupName) {
		if err != nil {
			return fmt.Errorf("error filtering entries: %w", err)
		}

		totalEntries++
		matchedEntries++
		entries = append(entries, entry)

		// Apply limit if specified (early termination advantage)
		if config.LimitEntries > 0 && matchedEntries >= config.LimitEntries {
			break
		}
	}

	// Count total entries for stats if needed (requires separate iteration)
	if config.ShowStats {
		for range reader.ReadEntriesIter() {
			totalEntries++
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatStreamingEntriesResult(entries, totalEntries, matchedEntries, queryTime, config)
}

func writeJSONLines[T any](entries []T, writer io.Writer) error {
	encoder := json.NewEncoder(writer)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
	}
	return nil
}

// formatStreamingGroupsResult formats groups output from streaming query
func formatStreamingGroupsResult(groups []buildkitelogs.GroupInfo, totalEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(groups, io.Writer(os.Stdout))
	}

	// Text format
	fmt.Fprintf(os.Stderr, "Groups found: %d\n\n", len(groups))

	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "No groups found.")
		return nil
	}

	// Print table header
	fmt.Printf("%-40s %8s %19s %19s\n",
		"GROUP NAME", "ENTRIES", "FIRST SEEN", "LAST SEEN")
	fmt.Println(strings.Repeat("-", 89))

	for _, group := range groups {
		fmt.Printf("%-40s %8d %19s %19s\n",
			truncateString(group.Name, 40),
			group.EntryCount,
			group.FirstSeen.Format("2006-01-02 15:04:05"),
			group.LastSeen.Format("2006-01-02 15:04:05"))
	}

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Query Statistics (Streaming) ---\n")
		fmt.Fprintf(os.Stderr, "Total entries: %d\n", totalEntries)
		fmt.Fprintf(os.Stderr, "Total groups: %d\n", len(groups))
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// formatSearchResultsLibrary formats search results with context lines using library types
func formatSearchResultsLibrary(results []buildkitelogs.SearchResult, matchesFound int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(results, os.Stdout)
	}

	// Output search results using consistent formatting
	if !config.RawOutput {
		limitText := ""
		if config.LimitEntries > 0 && matchesFound >= config.LimitEntries {
			limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
		}
		fmt.Fprintf(os.Stderr, "Matches found: %d%s\n\n", matchesFound, limitText)

		if len(results) == 0 {
			fmt.Fprintln(os.Stderr, "No matches found.")
		}
	}

	if len(results) > 0 {
		formatSearchResults(results, config)
	}

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Search Statistics (Streaming) ---\n")
		fmt.Fprintf(os.Stderr, "Matches found: %d\n", matchesFound)
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// formatStreamingEntriesResult formats entries output from streaming query
func formatStreamingEntriesResult(entries []buildkitelogs.ParquetLogEntry, totalEntries, matchedEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(entries, os.Stdout)
	}

	// Output entries using consistent formatting
	if !config.RawOutput {
		limitText := ""
		if config.LimitEntries > 0 && matchedEntries >= config.LimitEntries {
			limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
		}
		fmt.Fprintf(os.Stderr, "Entries in group matching '%s': %d%s\n\n", config.GroupName, matchedEntries, limitText)

		if len(entries) == 0 {
			fmt.Fprintln(os.Stderr, "No entries found for the specified group.")
		}
	}

	if len(entries) > 0 {
		formatLogEntries(entries, config)
	}

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Query Statistics (Streaming) ---\n")
		if totalEntries > 0 {
			fmt.Fprintf(os.Stderr, "Total entries: %d\n", totalEntries)
		}
		fmt.Fprintf(os.Stderr, "Matched entries: %d\n", matchedEntries)
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// showFileInfo displays metadata about the Parquet file
func showFileInfo(reader *buildkitelogs.ParquetReader, config *QueryConfig) error {
	info, err := reader.GetFileInfo()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if config.Format == "json" {
		return writeJSONLines([]*buildkitelogs.ParquetFileInfo{info}, os.Stdout)
	}

	// Text format
	fmt.Fprintf(os.Stderr, "Parquet File Information:\n")
	fmt.Fprintf(os.Stderr, "  File:         %s\n", config.ParquetFile)
	fmt.Fprintf(os.Stderr, "  Rows:         %d\n", info.RowCount)
	fmt.Fprintf(os.Stderr, "  Columns:      %d\n", info.ColumnCount)
	fmt.Fprintf(os.Stderr, "  File Size:    %d bytes (%.2f MB)\n", info.FileSize, float64(info.FileSize)/(1024*1024))
	fmt.Fprintf(os.Stderr, "  Row Groups:   %d\n", info.NumRowGroups)

	return nil
}

// tailFile shows the last N entries from the file
func tailFile(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	// Get file info to calculate starting position
	info, err := reader.GetFileInfo()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Calculate starting row for tail operation
	tailLines := int64(config.TailLines)
	if tailLines <= 0 {
		tailLines = 10 // Default to 10 lines
	}

	startRow := info.RowCount - tailLines
	if startRow < 0 {
		startRow = 0
	}

	var entries []buildkitelogs.ParquetLogEntry
	entriesRead := 0

	for entry, err := range reader.SeekToRow(startRow) {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		entries = append(entries, entry)
		entriesRead++

		// Limit to requested tail lines
		if entriesRead >= int(tailLines) {
			break
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatTailResult(entries, info.RowCount, int64(entriesRead), queryTime, config)
}

// seekToRow starts reading from a specific row
func seekToRow(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	var entries []buildkitelogs.ParquetLogEntry
	entriesRead := 0

	for entry, err := range reader.SeekToRow(config.SeekToRow) {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		entries = append(entries, entry)
		entriesRead++

		// Apply limit if specified
		if config.LimitEntries > 0 && entriesRead >= config.LimitEntries {
			break
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatSeekResult(entries, config.SeekToRow, int64(entriesRead), queryTime, config)
}

// formatTailResult formats tail command output
func formatTailResult(entries []buildkitelogs.ParquetLogEntry, totalRows, entriesRead int64, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(entries, os.Stdout)
	}

	// Output entries using consistent formatting
	if !config.RawOutput {
		fmt.Fprintf(os.Stderr, "Last %d entries:\n\n", entriesRead)
	}

	formatLogEntries(entries, config)

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Tail Statistics ---\n")
		fmt.Fprintf(os.Stderr, "Total rows in file: %d\n", totalRows)
		fmt.Fprintf(os.Stderr, "Entries shown: %d\n", entriesRead)
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// formatSeekResult formats seek command output
func formatSeekResult(entries []buildkitelogs.ParquetLogEntry, startRow, entriesRead int64, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(entries, os.Stdout)
	}

	// Output entries using consistent formatting
	if !config.RawOutput {
		limitText := ""
		if config.LimitEntries > 0 && entriesRead >= int64(config.LimitEntries) {
			limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
		}
		fmt.Fprintf(os.Stderr, "Entries starting from row %d: %d%s\n\n", startRow, entriesRead, limitText)
	}

	formatLogEntries(entries, config)

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Seek Statistics ---\n")
		fmt.Fprintf(os.Stderr, "Start row: %d\n", startRow)
		fmt.Fprintf(os.Stderr, "Entries shown: %d\n", entriesRead)
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// streamDump handles dump operation using streaming to output all entries
func streamDump(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	var entries []buildkitelogs.ParquetLogEntry
	totalEntries := 0

	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		totalEntries++
		entries = append(entries, entry)

		// Apply limit if specified (early termination advantage)
		if config.LimitEntries > 0 && totalEntries >= config.LimitEntries {
			break
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatDumpResult(entries, totalEntries, queryTime, config)
}

// formatDumpResult formats dump command output
func formatDumpResult(entries []buildkitelogs.ParquetLogEntry, totalEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		return writeJSONLines(entries, os.Stdout)
	}

	// Output entries using consistent formatting
	if !config.RawOutput {
		limitText := ""
		if config.LimitEntries > 0 && len(entries) >= config.LimitEntries {
			limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
		}
		fmt.Fprintf(os.Stderr, "Entries from file: %d%s\n\n", len(entries), limitText)
	}

	formatLogEntries(entries, config)

	if config.ShowStats {
		fmt.Fprintf(os.Stderr, "\n--- Dump Statistics ---\n")
		fmt.Fprintf(os.Stderr, "Total entries: %d\n", totalEntries)
		fmt.Fprintf(os.Stderr, "Entries shown: %d\n", len(entries))
		fmt.Fprintf(os.Stderr, "Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
