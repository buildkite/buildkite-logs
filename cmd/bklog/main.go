package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

type Config struct {
	FilePath    string
	OutputJSON  bool
	StripANSI   bool
	Filter      string
	ShowSummary bool
	ShowGroups  bool
	ParquetFile string
	UseSeq2     bool
	// Buildkite API parameters
	Organization string
	Pipeline     string
	Build        string
	Job          string
}

type ProcessingSummary struct {
	TotalEntries    int
	FilteredEntries int
	BytesProcessed  int64
	EntriesWithTime int
	Commands        int
	Sections        int
	Progress        int
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "parse":
		handleParseCommand()
	case "query":
		handleQueryCommand()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf("Usage: %s <subcommand> [options]\n\n", os.Args[0])
	fmt.Println("Subcommands:")
	fmt.Println("  parse   Parse Buildkite log files and export to various formats")
	fmt.Println("  query   Query Parquet log files")
	fmt.Println("  help    Show this help message")
	fmt.Println("")
	fmt.Printf("Use '%s <subcommand> -h' for subcommand-specific help", os.Args[0])
}

func handleParseCommand() {
	var config Config

	parseFlags := flag.NewFlagSet("parse", flag.ExitOnError)
	parseFlags.StringVar(&config.FilePath, "file", "", "Path to Buildkite log file (use this OR API parameters)")
	parseFlags.BoolVar(&config.OutputJSON, "json", false, "Output as JSON")
	parseFlags.BoolVar(&config.StripANSI, "strip-ansi", false, "Strip ANSI escape sequences from output")
	parseFlags.StringVar(&config.Filter, "filter", "", "Filter entries by type: command, progress, group")
	parseFlags.BoolVar(&config.ShowSummary, "summary", false, "Show processing summary at the end")
	parseFlags.BoolVar(&config.ShowGroups, "groups", false, "Show group/section information")
	parseFlags.StringVar(&config.ParquetFile, "parquet", "", "Export to Parquet file (e.g., output.parquet)")
	parseFlags.BoolVar(&config.UseSeq2, "use-seq2", false, "Use Go 1.23+ iter.Seq2 for iteration (experimental)")
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
		fmt.Printf("\n  # API:\n")
		fmt.Printf("  %s parse -org myorg -pipeline mypipe -build 123 -job abc-def -json\n", os.Args[0])
		fmt.Printf("  %s parse -org myorg -pipeline mypipe -build 123 -job abc-def -parquet logs.parquet\n", os.Args[0])
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

func handleQueryCommand() {
	var config QueryConfig

	queryFlags := flag.NewFlagSet("query", flag.ExitOnError)
	queryFlags.StringVar(&config.ParquetFile, "file", "", "Path to Parquet log file (required)")
	queryFlags.StringVar(&config.Operation, "op", "list-groups", "Query operation: list-groups, by-group")
	queryFlags.StringVar(&config.GroupName, "group", "", "Group name to filter by (for by-group operation)")
	queryFlags.StringVar(&config.Format, "format", "text", "Output format: text, json")
	queryFlags.BoolVar(&config.ShowStats, "stats", true, "Show query statistics")

	queryFlags.Usage = func() {
		fmt.Printf("Usage: %s query -file <parquet-file> [options]\n\n", os.Args[0])
		fmt.Println("Query Parquet log files.")
		fmt.Println("\nOptions:")
		queryFlags.PrintDefaults()
		fmt.Println("\nOperations:")
		fmt.Println("  list-groups  List all groups with statistics")
		fmt.Println("  by-group     Show entries for a specific group")
		fmt.Println("\nExamples:")
		fmt.Printf("  %s query -file logs.parquet -op list-groups\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op by-group -group \"Running tests\"\n", os.Args[0])
		fmt.Printf("  %s query -file logs.parquet -op list-groups -format json\n", os.Args[0])
	}

	if err := queryFlags.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	if config.ParquetFile == "" {
		queryFlags.Usage()
		os.Exit(1)
	}

	if err := runQuery(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// runQuery is now implemented in query_cli.go using the library package

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
		
		client := buildkitelogs.NewBuildkiteAPIClient(apiToken)
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

	// Handle Parquet export if specified
	if config.ParquetFile != "" {
		if config.UseSeq2 {
			err := exportToParquetSeq2(reader, parser, config.ParquetFile, config.Filter, summary)
			if err != nil {
				return fmt.Errorf("failed to export to Parquet: %w", err)
			}
		} else {
			err := exportToParquet(reader, parser, config.ParquetFile, config.Filter, summary)
			if err != nil {
				return fmt.Errorf("failed to export to Parquet: %w", err)
			}
		}
	} else {
		// Regular output processing
		if config.UseSeq2 {
			err := outputSeq2(reader, parser, config.OutputJSON, config.Filter, config.StripANSI, config.ShowGroups, summary)
			if err != nil {
				return fmt.Errorf("failed to process data: %w", err)
			}
		} else {
			iterator := parser.NewIterator(reader)
			if config.OutputJSON {
				err := outputJSONIterator(iterator, config.Filter, config.StripANSI, config.ShowGroups, summary)
				if err != nil {
					return fmt.Errorf("failed to process data: %w", err)
				}
			} else {
				err := outputTextIterator(iterator, config.Filter, config.StripANSI, config.ShowGroups, summary)
				if err != nil {
					return fmt.Errorf("failed to process data: %w", err)
				}
			}
		}
	}

	if config.ShowSummary {
		printSummary(summary)
	}

	return nil
}

func outputSeq2(reader io.Reader, parser *buildkitelogs.Parser, outputJSON bool, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {

	if outputJSON {
		return outputJSONSeq2(reader, parser, filter, stripANSI, showGroups, summary)
	}
	return outputTextSeq2(reader, parser, filter, stripANSI, showGroups, summary)
}

func outputJSONSeq2(reader io.Reader, parser *buildkitelogs.Parser, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
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
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}

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

func outputTextSeq2(reader io.Reader, parser *buildkitelogs.Parser, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
	for entry, err := range parser.All(reader) {
		if err != nil {
			return fmt.Errorf("parse error: %w", err)
		}

		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}

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
		return entry.IsCommand()
	case "group", "section": // Support both for backward compatibility
		return entry.IsGroup()
	case "progress":
		return entry.IsProgress()
	default:
		return true
	}
}

func outputJSONIterator(iterator *buildkitelogs.LogIterator, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
	type JSONEntry struct {
		Timestamp string `json:"timestamp,omitempty"`
		Content   string `json:"content"`
		HasTime   bool   `json:"has_timestamp"`
		Group     string `json:"group,omitempty"`
	}

	var jsonEntries []JSONEntry

	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}

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

	if err := iterator.Err(); err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jsonEntries)
}

func outputTextIterator(iterator *buildkitelogs.LogIterator, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}

		if !shouldIncludeEntry(entry, filter) {
			continue
		}

		summary.FilteredEntries++

		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}

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

	return iterator.Err()
}

func exportToParquet(reader io.Reader, parser *buildkitelogs.Parser, filename string, filter string, summary *ProcessingSummary) error {
	iterator := parser.NewIterator(reader)

	// Create filter function based on filter string
	var filterFunc func(*buildkitelogs.LogEntry) bool
	if filter != "" {
		filterFunc = func(entry *buildkitelogs.LogEntry) bool {
			return shouldIncludeEntry(entry, filter)
		}
	}

	// Count entries for summary while iterating
	var entries []*buildkitelogs.LogEntry
	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++

		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}

		// Apply filter if specified
		if filterFunc == nil || filterFunc(entry) {
			summary.FilteredEntries++
			entries = append(entries, entry)
		}
	}

	if err := iterator.Err(); err != nil {
		return err
	}

	return buildkitelogs.ExportToParquet(entries, filename)
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
			if entry.IsCommand() {
				summary.Commands++
			}
			if entry.IsGroup() {
				summary.Sections++
			}
			if entry.IsProgress() {
				summary.Progress++
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

func printSummary(summary *ProcessingSummary) {
	fmt.Printf("\n--- Processing Summary ---\n")
	if summary.BytesProcessed >= 0 {
		fmt.Printf("Bytes processed: %.1f KB\n", float64(summary.BytesProcessed)/1024)
	} else {
		fmt.Printf("Bytes processed: (API source - unknown)\n")
	}
	fmt.Printf("Total entries: %d\n", summary.TotalEntries)
	fmt.Printf("Entries with timestamps: %d\n", summary.EntriesWithTime)
	fmt.Printf("Commands: %d\n", summary.Commands)
	fmt.Printf("Sections: %d\n", summary.Sections)
	fmt.Printf("Progress updates: %d\n", summary.Progress)
	fmt.Printf("Regular output: %d\n", summary.TotalEntries-summary.Commands-summary.Sections-summary.Progress)

	if summary.FilteredEntries > 0 {
		fmt.Printf("Exported %d entries to %s\n", summary.FilteredEntries, "Parquet file")
	}
}
