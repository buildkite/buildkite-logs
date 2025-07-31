package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
	// Example: Using the ParquetReader to stream query Buildkite logs

	// Create a new reader for your Parquet file
	reader := buildkitelogs.NewParquetReader("../../test_logs.parquet")

	fmt.Println("🔍 Buildkite Logs Parquet Streaming Query Example")
	fmt.Println(strings.Repeat("=", 50))

	// Example 1: Stream through all entries and build group statistics
	fmt.Println("\n📊 Streaming all log entries and building group statistics:")
	groupMap := make(map[string]*buildkitelogs.GroupInfo)
	totalEntries := 0

	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			log.Fatalf("Failed to read entries: %v", err)
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

	fmt.Printf("Processed %d total entries\n", totalEntries)
	i := 1
	for name, info := range groupMap {
		if i <= 3 { // Show first 3 groups
			fmt.Printf("%d. %s (%d entries)\n",
				i, name, info.EntryCount)
		}
		i++
		if i > 3 {
			break
		}
	}

	// Example 2: Stream filter by group pattern
	fmt.Println("\n🔎 Streaming entries containing 'environment':")
	environmentCount := 0

	for entry, err := range reader.FilterByGroupIter("environment") {
		if err != nil {
			log.Fatalf("Failed to filter by group: %v", err)
		}

		environmentCount++
		if environmentCount <= 3 { // Show first 3 entries
			fmt.Printf("  [%s] %s\n", entry.Group, entry.Content)
		}
	}

	fmt.Printf("Found %d entries containing 'environment'\n", environmentCount)

	// Example 3: Direct file streaming
	fmt.Println("\n📖 Streaming entries directly from file:")
	directCount := 0

	for _, err := range buildkitelogs.ReadParquetFileIter("../../test_logs.parquet") {
		if err != nil {
			log.Fatalf("Failed to read Parquet file: %v", err)
		}

		directCount++
		if directCount > 1000 { // Limit for demo
			break
		}
	}

	fmt.Printf("Streamed %d entries from Parquet file\n", directCount)

	// Example 4: Memory-efficient filtering with early termination
	fmt.Println("\n🔧 Streaming with early termination:")
	testCount := 0

	for _, err := range reader.FilterByGroupIter("test") {
		if err != nil {
			log.Fatalf("Failed to filter entries: %v", err)
		}

		testCount++
		// Early termination after finding 5 entries
		if testCount >= 5 {
			break
		}
	}

	fmt.Printf("Found %d test-related entries (stopped early for demo)\n", testCount)

	fmt.Println("\n✅ Streaming example completed successfully!")
	fmt.Println("💡 All operations used constant memory regardless of file size")
}
