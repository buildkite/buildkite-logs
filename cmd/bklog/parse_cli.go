package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	buildkitelogs "github.com/buildkite/buildkite-logs"
)

func handleParseCommand() {
	var config Config

	parseFlags := flag.NewFlagSet("parse", flag.ExitOnError)
	parseFlags.StringVar(&config.FilePath, "file", "", "Path to Buildkite log file (use this OR API parameters)")
	parseFlags.BoolVar(&config.OutputJSON, "json", false, "Output as JSON")
	parseFlags.StringVar(&config.Filter, "filter", "", "Filter entries by type: command, group")
	parseFlags.BoolVar(&config.ShowSummary, "summary", false, "Show processing summary at the end")
	parseFlags.BoolVar(&config.ShowGroups, "groups", false, "Show group/section information")
	parseFlags.StringVar(&config.ParquetFile, "parquet", "", "Export to Parquet file (e.g., output.parquet)")
	parseFlags.StringVar(&config.JSONLFile, "jsonl", "", "Export to JSON Lines file (e.g., output.jsonl)")
	// Buildkite API parameters
	parseFlags.StringVar(&config.Organization, "org", "", "Buildkite organization slug (for API)")
	parseFlags.StringVar(&config.Pipeline, "pipeline", "", "Buildkite pipeline slug (for API)")
	parseFlags.StringVar(&config.Build, "build", "", "Buildkite build number or UUID (for API)")
	parseFlags.StringVar(&config.Job, "job", "", "Buildkite job ID (for API)")

	parseFlags.Usage = func() {
		fmt.Printf("Usage: %s parse [options]\n\n", os.Args[0])
		fmt.Println("Parse Buildkite log files from local files or API and export to various formats.")
		fmt.Println("\nYou must provide either:")
		fmt.Println("  -file <path>     Local log file")
		fmt.Println("  OR API params:   -org -pipeline -build -job")
		fmt.Println("\nFor API usage, set BUILDKITE_API_TOKEN environment variable.")
		fmt.Println("\nOptions:")
		parseFlags.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Printf("  # Local file:\n")
		fmt.Printf("  %s parse -file buildkite.log -strip-ansi\n", os.Args[0])
		fmt.Printf("  %s parse -file buildkite.log -filter command -json\n", os.Args[0])
		fmt.Printf("  %s parse -file buildkite.log -parquet output.parquet -summary\n", os.Args[0])
		fmt.Printf("  %s parse -file buildkite.log -jsonl output.jsonl -summary\n", os.Args[0])
		fmt.Printf("\n  # API:\n")
		fmt.Printf("  %s parse -org myorg -pipeline mypipe -build 123 -job abc-def -json\n", os.Args[0])
		fmt.Printf("  %s parse -org myorg -pipeline mypipe -build 123 -job abc-def -parquet logs.parquet\n", os.Args[0])
		fmt.Printf("  %s parse -org myorg -pipeline mypipe -build 123 -job abc-def -jsonl logs.jsonl\n", os.Args[0])
	}

	if err := parseFlags.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	// Validate that either file or API parameters are provided
	hasFile := config.FilePath != ""
	hasAPIParams := config.Organization != "" || config.Pipeline != "" || config.Build != "" || config.Job != ""

	if !hasFile && !hasAPIParams {
		fmt.Fprintf(os.Stderr, "Error: Must provide either -file or API parameters (-org, -pipeline, -build, -job)\n\n")
		parseFlags.Usage()
		os.Exit(1)
	}

	if hasFile && hasAPIParams {
		fmt.Fprintf(os.Stderr, "Error: Cannot use both -file and API parameters simultaneously\n\n")
		parseFlags.Usage()
		os.Exit(1)
	}

	// If using API, validate all required parameters are present
	if hasAPIParams {
		if err := buildkitelogs.ValidateAPIParams(config.Organization, config.Pipeline, config.Build, config.Job); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
			parseFlags.Usage()
			os.Exit(1)
		}
	}

	if err := runParse(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runParse(config *Config) error {
	var reader io.ReadCloser
	var bytesProcessed int64

	// Determine data source: file or API
	if config.FilePath != "" {
		// Local file
		file, err := os.Open(config.FilePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		reader = file

		// Get file size for bytes processed calculation
		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to get file info: %w", err)
		}
		bytesProcessed = fileInfo.Size()
	} else {
		// Buildkite API
		apiToken := os.Getenv("BUILDKITE_API_TOKEN")
		if apiToken == "" {
			return fmt.Errorf("BUILDKITE_API_TOKEN environment variable is required for API access")
		}

		client := buildkitelogs.NewBuildkiteAPIClient(apiToken, version)
		logReader, err := client.GetJobLog(config.Organization, config.Pipeline, config.Build, config.Job)
		if err != nil {
			return fmt.Errorf("failed to fetch logs from API: %w", err)
		}
		reader = logReader
		bytesProcessed = -1 // Unknown for API
	}

	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close reader: %v\n", closeErr)
		}
	}()

	summary := &ProcessingSummary{
		BytesProcessed: bytesProcessed,
	}

	parser := buildkitelogs.NewParser()

	// Handle export options
	switch {
	case config.ParquetFile != "":
		err := exportToParquetSeq2(reader, parser, config.ParquetFile, config.Filter, summary)
		if err != nil {
			return fmt.Errorf("failed to export to Parquet: %w", err)
		}
	case config.JSONLFile != "":
		err := exportToJSONLSeq2(reader, parser, config.JSONLFile, config.Filter, summary)
		if err != nil {
			return fmt.Errorf("failed to export to JSON Lines: %w", err)
		}
	default:
		// Regular output processing
		err := outputSeq2(reader, parser, config.OutputJSON, config.Filter, config.ShowGroups, summary)
		if err != nil {
			return fmt.Errorf("failed to process data: %w", err)
		}
	}

	if config.ShowSummary {
		printSummary(summary)
	}

	return nil
}

func outputSeq2(reader io.Reader, parser *buildkitelogs.Parser, outputJSON bool, filter string, showGroups bool, summary *ProcessingSummary) error {

	if outputJSON {
		return outputJSONSeq2(reader, parser, filter, showGroups, summary)
	}
	return outputTextSeq2(reader, parser, filter, showGroups, summary)
}

func outputJSONSeq2(reader io.Reader, parser *buildkitelogs.Parser, filter string, showGroups bool, summary *ProcessingSummary) error {
	type JSONEntry struct {
		Timestamp string `json:"timestamp,omitempty"`
		Content   string `json:"content"`
		HasTime   bool   `json:"has_timestamp"`
		Group     string `json:"group,omitempty"`
	}

	var jsonEntries []JSONEntry

	for entry, err := range parser.All(reader) {
		if err != nil {
			return fmt.Errorf("parse error: %w", err)
		}

		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}

		if entry.IsGroup() {
			summary.Sections++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content

		jsonEntry := JSONEntry{
			Content: content,
			HasTime: entry.HasTimestamp(),
		}

		if entry.HasTimestamp() {
			jsonEntry.Timestamp = entry.Timestamp.Format("2006-01-02T15:04:05.000Z")
		}

		if showGroups && entry.Group != "" {
			jsonEntry.Group = entry.Group
		}

		jsonEntries = append(jsonEntries, jsonEntry)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jsonEntries)
}

func outputTextSeq2(reader io.Reader, parser *buildkitelogs.Parser, filter string, showGroups bool, summary *ProcessingSummary) error {
	for entry, err := range parser.All(reader) {
		if err != nil {
			return fmt.Errorf("parse error: %w", err)
		}

		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}

		if entry.IsGroup() {
			summary.Sections++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content

		if showGroups && entry.Group != "" {
			if entry.HasTimestamp() {
				fmt.Printf("[%s] [%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05.000"), entry.Group, content)
			} else {
				fmt.Printf("[%s] %s\n", entry.Group, content)
			}
		} else {
			if entry.HasTimestamp() {
				fmt.Printf("[%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05.000"), content)
			} else {
				fmt.Printf("%s\n", content)
			}
		}
	}

	return nil
}

func shouldIncludeEntry(entry *buildkitelogs.LogEntry, filter string) bool {
	switch filter {
	case "command":
		return false // Commands filter no longer supported
	case "group", "section": // Support both for backward compatibility
		return entry.IsGroup()
	default:
		return true
	}
}

func exportToParquetSeq2(reader io.Reader, parser *buildkitelogs.Parser, filename string, filter string, summary *ProcessingSummary) error {
	// Create filter function based on filter string
	var filterFunc func(*buildkitelogs.LogEntry) bool
	if filter != "" {
		filterFunc = func(entry *buildkitelogs.LogEntry) bool {
			return shouldIncludeEntry(entry, filter)
		}
	}

	// Create a sequence that counts entries for summary and handles errors
	countingSeq := func(yield func(*buildkitelogs.LogEntry, error) bool) {
		lineNum := 0
		for entry, err := range parser.All(reader) {
			lineNum++

			// Handle parse errors - still count them but log warnings
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error parsing line %d: %v\n", lineNum, err)
				if !yield(nil, err) {
					return
				}
				continue
			}

			summary.TotalEntries++

			// Update entry type counts
			if entry.HasTimestamp() {
				summary.EntriesWithTime++
			}

			if entry.IsGroup() {
				summary.Sections++
			}

			// Apply filter if specified
			if filterFunc == nil || filterFunc(entry) {
				summary.FilteredEntries++
			}

			// Always yield the entry for export consideration
			if !yield(entry, nil) {
				return
			}
		}
	}

	// Export using the Seq2 iterator with filtering
	return buildkitelogs.ExportSeq2ToParquetWithFilter(countingSeq, filename, filterFunc)
}

func exportToJSONLSeq2(reader io.Reader, parser *buildkitelogs.Parser, filename string, filter string, summary *ProcessingSummary) error {
	// Create filter function based on filter string
	var filterFunc func(*buildkitelogs.LogEntry) bool
	if filter != "" {
		filterFunc = func(entry *buildkitelogs.LogEntry) bool {
			return shouldIncludeEntry(entry, filter)
		}
	}

	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON Lines file: %w", err)
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)

	// Create a sequence that counts entries for summary and handles errors
	lineNum := 0
	for entry, err := range parser.All(reader) {
		lineNum++

		// Handle parse errors - still count them but log warnings
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error parsing line %d: %v\n", lineNum, err)
			continue
		}

		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}

		if entry.IsGroup() {
			summary.Sections++
		}

		// Apply filter if specified
		if filterFunc == nil || filterFunc(entry) {
			summary.FilteredEntries++

			// Create JSON Lines record
			record := map[string]any{
				"timestamp": entry.Timestamp.UnixMilli(),
				"content":   entry.Content,
				"group":     entry.Group,
				"flags":     int32(entry.ComputeFlags()),
			}

			if err := encoder.Encode(record); err != nil {
				return fmt.Errorf("failed to write JSON Lines record: %w", err)
			}
		}
	}

	return nil
}

func printSummary(summary *ProcessingSummary) {
	fmt.Printf("\n--- Processing Summary ---\n")
	if summary.BytesProcessed >= 0 {
		fmt.Printf("Bytes processed: %.1f KB\n", float64(summary.BytesProcessed)/1024)
	} else {
		fmt.Printf("Bytes processed: (API source - unknown)\n")
	}
	fmt.Printf("Total entries: %d\n", summary.TotalEntries)
	fmt.Printf("Entries with timestamps: %d\n", summary.EntriesWithTime)
	fmt.Printf("Sections: %d\n", summary.Sections)
	fmt.Printf("Regular output: %d\n", summary.TotalEntries-summary.Sections)

	if summary.FilteredEntries > 0 {
		fmt.Printf("Exported %d entries to %s\n", summary.FilteredEntries, "Parquet file")
	}
}
