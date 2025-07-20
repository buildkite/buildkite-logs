package main

import (
	"fmt"
	"os"
)

// version can be overridden at build time using:
// go build -ldflags "-X main.version=v1.2.3" ./cmd/bklog
var version = "dev"

type Config struct {
	FilePath    string
	OutputJSON  bool
	Filter      string
	ShowSummary bool
	ShowGroups  bool
	ParquetFile string
	JSONLFile   string
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
	case "debug":
		handleDebugCommand()
	case "version", "-v", "--version":
		fmt.Printf("bklog version %s\n", version)
		return
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
	fmt.Println("  parse     Parse Buildkite log files and export to various formats")
	fmt.Println("  query     Query Parquet log files (supports local files and Buildkite API)")
	fmt.Println("  debug     Debug parser issues with raw log inspection")
	fmt.Println("  version   Show version information")
	fmt.Println("  help      Show this help message")
	fmt.Println("")
	fmt.Printf("Use '%s <subcommand> -h' for subcommand-specific help", os.Args[0])
}

// handleQueryCommand is now implemented in query_cli.go using the library package
