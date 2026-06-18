package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/buildkite/buildkite-logs/logparser"
)

type DebugConfig struct {
	LogFile           string
	Mode              string
	StartLine         int
	EndLine           int
	Limit             int
	ShowHex           bool
	ShowRaw           bool
	ShowParsed        bool
	Verbose           bool
	CSVOutput         string
	MaxLineBytes      int
	TruncateLongLines bool
}

func handleDebugCommand() {
	var config DebugConfig

	debugFlags := flag.NewFlagSet("debug", flag.ExitOnError)
	debugFlags.StringVar(&config.LogFile, "file", "", "Path to log file (required)")
	debugFlags.StringVar(&config.Mode, "mode", "parse", "Debug mode: parse, hex, lines, extract-timestamps")
	debugFlags.IntVar(&config.StartLine, "start", 1, "Start line number (1-based)")
	debugFlags.IntVar(&config.EndLine, "end", 0, "End line number (0 = start+limit or EOF)")
	debugFlags.IntVar(&config.Limit, "limit", 10, "Number of lines to process")
	debugFlags.BoolVar(&config.ShowHex, "hex", false, "Show hex dump of each line")
	debugFlags.BoolVar(&config.ShowRaw, "raw", false, "Show raw line content")
	debugFlags.BoolVar(&config.ShowParsed, "parsed", true, "Show parsed log entry")
	debugFlags.BoolVar(&config.Verbose, "verbose", false, "Show detailed parsing information")
	debugFlags.StringVar(&config.CSVOutput, "csv", "", "Output CSV file for extract-timestamps mode")
	debugFlags.IntVar(&config.MaxLineBytes, "max-line-bytes", logparser.DefaultMaxLineBytes, "Maximum bytes allowed in a single log line")
	debugFlags.BoolVar(&config.TruncateLongLines, "truncate-long-lines", false, "Truncate log lines that exceed -max-line-bytes instead of returning an error")

	debugFlags.Usage = func() {
		fmt.Printf("Usage: %s debug [options]\n\n", os.Args[0])
		fmt.Println("Debug parser issues with raw log inspection.")
		fmt.Println("\nOptions:")
		debugFlags.PrintDefaults()
		fmt.Println("\nModes:")
		fmt.Println("  parse              Parse lines and show results (default)")
		fmt.Println("  hex                Show hex dump of lines")
		fmt.Println("  lines              Show raw line content with line numbers")
		fmt.Println("  extract-timestamps Extract all OSC timestamps to CSV")
		fmt.Println("\nExamples:")
		fmt.Printf("  %s debug -file logs.log -start 1 -limit 5\n", os.Args[0])
		fmt.Printf("  %s debug -file logs.log -mode hex -start 100 -limit 2\n", os.Args[0])
		fmt.Printf("  %s debug -file logs.log -start 50 -end 55 -verbose\n", os.Args[0])
		fmt.Printf("  %s debug -file logs.log -mode extract-timestamps -csv timestamps.csv\n", os.Args[0])
	}

	if err := debugFlags.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	if config.LogFile == "" {
		fmt.Fprintf(os.Stderr, "Error: -file is required\n\n")
		debugFlags.Usage()
		os.Exit(1)
	}

	if config.Mode == "extract-timestamps" && config.CSVOutput == "" {
		fmt.Fprintf(os.Stderr, "Error: -csv is required for extract-timestamps mode\n\n")
		debugFlags.Usage()
		os.Exit(1)
	}

	if err := runDebug(&config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runDebug(config *DebugConfig) error {
	file, err := os.Open(config.LogFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Handle extract-timestamps mode separately
	if config.Mode == "extract-timestamps" {
		return extractTimestampsToCSV(config)
	}

	parserOptions := debugParserOptions(config)
	parser := logparser.New(
		logparser.WithMaxLineBytes(parserOptions.MaxLineBytes),
		logparser.WithTruncateLongLines(parserOptions.TruncateLongLines),
	)
	lineReader := logparser.NewLineReader(file, parserOptions)
	processed := 0

	// Calculate end line
	endLine := config.EndLine
	if endLine == 0 {
		if config.Limit > 0 {
			endLine = config.StartLine + config.Limit - 1
		} else {
			endLine = int(^uint(0) >> 1) // Max int
		}
	}

	fmt.Printf("=== Debug Mode: %s ===\n", config.Mode)
	fmt.Printf("File: %s\n", config.LogFile)
	fmt.Printf("Lines: %d-%d\n\n", config.StartLine, endLine)

	for {
		line, err := lineReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		// Skip lines before start
		if line.Number < config.StartLine {
			continue
		}

		// Stop after end line or limit reached
		if line.Number > endLine || (config.Limit > 0 && processed >= config.Limit) {
			break
		}

		processed++

		lineText := string(line.Bytes)
		fmt.Printf("--- Line %d", line.Number)
		if line.Truncated {
			fmt.Printf(" (truncated)")
		}
		fmt.Println(" ---")

		switch config.Mode {
		case "hex":
			showHexDump(lineText)
		case "lines":
			showRawLine(lineText)
		case "parse":
			if err := showParseDebug(parser, line, config); err != nil {
				printParseError(err)
			}
		default:
			if err := showParseDebug(parser, line, config); err != nil {
				printParseError(err)
			}
		}

		fmt.Println()
	}

	fmt.Printf("Processed %d lines\n", processed)
	return nil
}

func debugParserOptions(config *DebugConfig) logparser.Options {
	options := logparser.DefaultOptions()
	options.MaxLineBytes = config.MaxLineBytes
	options.TruncateLongLines = config.TruncateLongLines
	return options
}

// extractTimestampsToCSV extracts all OSC timestamps from the log file and exports to CSV
func extractTimestampsToCSV(config *DebugConfig) error {
	file, err := os.Open(config.LogFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	csvFile, err := os.Create(config.CSVOutput)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV header
	if err := writer.Write([]string{"line_number", "osc_offset", "timestamp_ms", "timestamp_formatted"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	lineReader := logparser.NewLineReader(file, debugParserOptions(config))
	totalTimestamps := 0

	for {
		line, err := lineReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		// Extract all OSC sequences from this line
		timestamps := extractAllOSCTimestamps(line.Bytes, line.Number)
		totalTimestamps += len(timestamps)

		// Write each timestamp to CSV
		for _, ts := range timestamps {
			record := []string{
				strconv.Itoa(ts.LineNumber),
				strconv.Itoa(ts.Offset),
				strconv.FormatInt(ts.TimestampMs, 10),
				ts.FormattedTime,
			}
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("failed to write CSV record: %w", err)
			}
		}
	}

	fmt.Printf("Extracted %d timestamps to %s\n", totalTimestamps, config.CSVOutput)
	return nil
}

// TimestampRecord represents a single OSC timestamp extraction
type TimestampRecord struct {
	LineNumber    int
	Offset        int
	TimestampMs   int64
	FormattedTime string
}

// extractAllOSCTimestamps finds all OSC sequences in a line and extracts their timestamps
func extractAllOSCTimestamps(line []byte, lineNum int) []TimestampRecord {
	var results []TimestampRecord
	oscPattern := []byte{0x1b, '_', 'b', 'k', ';', 't', '='}

	searchStart := 0
	for {
		// Find next OSC sequence start
		idx := bytes.Index(line[searchStart:], oscPattern)
		if idx == -1 {
			break
		}

		absoluteOffset := searchStart + idx
		timestampStart := absoluteOffset + len(oscPattern)

		// Find the BEL terminator
		belIdx := bytes.IndexByte(line[timestampStart:], 0x07)
		if belIdx == -1 {
			// No BEL found, skip this sequence
			searchStart = timestampStart
			continue
		}

		timestampEnd := timestampStart + belIdx
		timestampBytes := line[timestampStart:timestampEnd]

		// Parse the timestamp
		timestampMs, err := strconv.ParseInt(string(timestampBytes), 10, 64)
		if err != nil {
			// Invalid timestamp, skip this sequence
			searchStart = timestampEnd + 1
			continue
		}

		// Convert to formatted time
		timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))
		formattedTime := timestamp.Format("2006-01-02T15:04:05.000Z")

		results = append(results, TimestampRecord{
			LineNumber:    lineNum,
			Offset:        absoluteOffset,
			TimestampMs:   timestampMs,
			FormattedTime: formattedTime,
		})

		// Continue search after this timestamp
		searchStart = timestampEnd + 1
	}

	return results
}

func showHexDump(line string) {
	data := []byte(line)
	fmt.Printf("Length: %d bytes\n", len(data))

	// Print hex dump similar to hexdump -C
	for i := 0; i < len(data); i += 16 {
		// Address
		fmt.Printf("%08x  ", i)

		// Hex bytes
		for j := 0; j < 16; j++ {
			if i+j < len(data) {
				fmt.Printf("%02x ", data[i+j])
			} else {
				fmt.Printf("   ")
			}
			if j == 7 {
				fmt.Printf(" ")
			}
		}

		// ASCII representation
		fmt.Printf(" |")
		for j := 0; j < 16 && i+j < len(data); j++ {
			b := data[i+j]
			if b >= 32 && b <= 126 {
				fmt.Printf("%c", b)
			} else {
				fmt.Printf(".")
			}
		}
		fmt.Printf("|\n")
	}
}

func showRawLine(line string) {
	fmt.Printf("Raw: %q\n", line)
	fmt.Printf("Length: %d\n", len(line))
}

func showParseDebug(parser *logparser.Parser, line logparser.Line, config *DebugConfig) error {
	lineText := string(line.Bytes)
	if config.ShowRaw {
		fmt.Printf("Raw: %q\n", lineText)
	}

	if config.ShowHex {
		showHexDump(lineText)
	}

	if config.ShowParsed {
		entry, err := parser.ParseLineBytes(line.Bytes, line)
		if err != nil {
			return err
		}

		if config.Verbose {
			fmt.Printf("Timestamp: %v (Unix: %d)\n", entry.Timestamp, entry.Timestamp.Unix())
			fmt.Printf("Content: %q\n", entry.Content)
			fmt.Printf("Group: %q\n", entry.Group)
			fmt.Printf("RawLine length: %d\n", len(entry.RawLine))
			fmt.Printf("IsGroup: %v\n", entry.IsGroup())
		} else {
			if !entry.Timestamp.IsZero() {
				fmt.Printf("[%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05.000"), entry.Content)
			} else {
				fmt.Printf("[NO TIMESTAMP] %s\n", entry.Content)
			}
			if entry.Group != "" && entry.Group != entry.Content {
				fmt.Printf("  Group: %s\n", entry.Group)
			}
		}
	}

	return nil
}

func printParseError(err error) {
	var parseErr *logparser.ParseError
	if errors.As(err, &parseErr) {
		fmt.Printf("Parse error: %s\n", parseErr.Kind)
		fmt.Printf("  Line: %d\n", parseErr.Line)
		fmt.Printf("  Stream offset: %d\n", parseErr.StreamOffset)
		fmt.Printf("  Line offset: %d\n", parseErr.LineOffset)
		fmt.Printf("  Before: %q\n", string(parseErr.Before))
		fmt.Printf("  After: %q\n", string(parseErr.After))
		if parseErr.Err != nil {
			fmt.Printf("  Cause: %v\n", parseErr.Err)
		}
		return
	}

	fmt.Printf("Parse error: %v\n", err)
}
