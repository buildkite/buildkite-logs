package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

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
	// Buildkite API parameters
	Organization string
	Pipeline     string
	Build        string
	Job          string
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

	// If API parameters are provided, download and cache
	if config.Organization != "" && config.Pipeline != "" && config.Build != "" && config.Job != "" {
		apiToken := os.Getenv("BUILDKITE_API_TOKEN")
		if apiToken == "" {
			return "", fmt.Errorf("BUILDKITE_API_TOKEN environment variable is required for API access")
		}

		cacheFilePath, err := buildkitelogs.DownloadAndCache(apiToken, config.Organization, config.Pipeline, config.Build, config.Job, version)
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
	case "by-group":
		if config.GroupName == "" {
			return fmt.Errorf("group pattern is required for by-group operation")
		}
		return streamByGroup(reader, config, start)
	case "info":
		return showFileInfo(reader, config)
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

		if entry.IsCommand {
			info.Commands++
		}
		if entry.IsProgress {
			info.Progress++
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

// formatStreamingGroupsResult formats groups output from streaming query
func formatStreamingGroupsResult(groups []buildkitelogs.GroupInfo, totalEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		result := struct {
			Groups []buildkitelogs.GroupInfo `json:"groups"`
			Stats  struct {
				TotalEntries int     `json:"total_entries"`
				TotalGroups  int     `json:"total_groups"`
				QueryTime    float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Groups: groups,
		}

		if config.ShowStats {
			result.Stats.TotalEntries = totalEntries
			result.Stats.TotalGroups = len(groups)
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	fmt.Fprintf(os.Stderr, "Groups found: %d\n\n", len(groups))

	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "No groups found.")
		return nil
	}

	// Print table header
	fmt.Printf("%-40s %8s %8s %8s %19s %19s\n",
		"GROUP NAME", "ENTRIES", "COMMANDS", "PROGRESS", "FIRST SEEN", "LAST SEEN")
	fmt.Println(strings.Repeat("-", 120))

	for _, group := range groups {
		fmt.Printf("%-40s %8d %8d %8d %19s %19s\n",
			truncateString(group.Name, 40),
			group.EntryCount,
			group.Commands,
			group.Progress,
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

// formatStreamingEntriesResult formats entries output from streaming query
func formatStreamingEntriesResult(entries []buildkitelogs.ParquetLogEntry, totalEntries, matchedEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		result := struct {
			Entries []buildkitelogs.ParquetLogEntry `json:"entries"`
			Stats   struct {
				TotalEntries   int     `json:"total_entries"`
				MatchedEntries int     `json:"matched_entries"`
				QueryTime      float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Entries: entries,
		}

		if config.ShowStats {
			result.Stats.TotalEntries = totalEntries
			result.Stats.MatchedEntries = matchedEntries
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	limitText := ""
	if config.LimitEntries > 0 && matchedEntries >= config.LimitEntries {
		limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
	}
	fmt.Fprintf(os.Stderr, "Entries in group matching '%s': %d%s\n\n", config.GroupName, matchedEntries, limitText)

	if len(entries) == 0 {
		fmt.Fprintln(os.Stderr, "No entries found for the specified group.")
		return nil
	}

	for _, entry := range entries {
		timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))

		var markers []string
		if entry.IsCommand {
			markers = append(markers, "CMD")
		}
		if entry.IsGroup {
			markers = append(markers, "GRP")
		}
		if entry.IsProgress {
			markers = append(markers, "PROG")
		}

		markerStr := ""
		if len(markers) > 0 {
			markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
		}

		fmt.Printf("[%s]%s %s\n",
			timestamp.Format("2006-01-02 15:04:05.000"),
			markerStr,
			entry.Content)
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
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(info)
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
		result := struct {
			Entries []buildkitelogs.ParquetLogEntry `json:"entries"`
			Stats   struct {
				TotalRows    int64   `json:"total_rows"`
				EntriesShown int64   `json:"entries_shown"`
				QueryTime    float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Entries: entries,
		}

		if config.ShowStats {
			result.Stats.TotalRows = totalRows
			result.Stats.EntriesShown = entriesRead
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	fmt.Fprintf(os.Stderr, "Last %d entries:\n\n", entriesRead)

	for _, entry := range entries {
		timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))

		var markers []string
		if entry.IsCommand {
			markers = append(markers, "CMD")
		}
		if entry.IsGroup {
			markers = append(markers, "GRP")
		}
		if entry.IsProgress {
			markers = append(markers, "PROG")
		}

		markerStr := ""
		if len(markers) > 0 {
			markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
		}

		fmt.Printf("[%s]%s %s\n",
			timestamp.Format("2006-01-02 15:04:05.000"),
			markerStr,
			entry.Content)
	}

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
		result := struct {
			Entries []buildkitelogs.ParquetLogEntry `json:"entries"`
			Stats   struct {
				StartRow     int64   `json:"start_row"`
				EntriesShown int64   `json:"entries_shown"`
				QueryTime    float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Entries: entries,
		}

		if config.ShowStats {
			result.Stats.StartRow = startRow
			result.Stats.EntriesShown = entriesRead
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	limitText := ""
	if config.LimitEntries > 0 && entriesRead >= int64(config.LimitEntries) {
		limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
	}
	fmt.Fprintf(os.Stderr, "Entries starting from row %d: %d%s\n\n", startRow, entriesRead, limitText)

	for _, entry := range entries {
		timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))

		var markers []string
		if entry.IsCommand {
			markers = append(markers, "CMD")
		}
		if entry.IsGroup {
			markers = append(markers, "GRP")
		}
		if entry.IsProgress {
			markers = append(markers, "PROG")
		}

		markerStr := ""
		if len(markers) > 0 {
			markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
		}

		fmt.Printf("[%s]%s %s\n",
			timestamp.Format("2006-01-02 15:04:05.000"),
			markerStr,
			entry.Content)
	}

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
		result := struct {
			Entries []buildkitelogs.ParquetLogEntry `json:"entries"`
			Stats   struct {
				TotalEntries int     `json:"total_entries"`
				EntriesShown int     `json:"entries_shown"`
				QueryTime    float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Entries: entries,
		}

		if config.ShowStats {
			result.Stats.TotalEntries = totalEntries
			result.Stats.EntriesShown = len(entries)
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	limitText := ""
	if config.LimitEntries > 0 && len(entries) >= config.LimitEntries {
		limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
	}
	fmt.Fprintf(os.Stderr, "Entries from file: %d%s\n\n", len(entries), limitText)

	for _, entry := range entries {
		timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))

		var markers []string
		if entry.IsCommand {
			markers = append(markers, "CMD")
		}
		if entry.IsGroup {
			markers = append(markers, "GRP")
		}
		if entry.IsProgress {
			markers = append(markers, "PROG")
		}

		markerStr := ""
		if len(markers) > 0 {
			markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
		}

		fmt.Printf("[%s]%s %s\n",
			timestamp.Format("2006-01-02 15:04:05.000"),
			markerStr,
			entry.Content)
	}

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
