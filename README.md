# Buildkite Logs Parser

A Go library and CLI tool for parsing Buildkite log files that contain OSC (Operating System Command) sequences with timestamps.

[![Build status](https://badge.buildkite.com/e17b73d584291c31c6a95c657687d9049d225b93d9f3c3fcd2.svg)](https://buildkite.com/mark-at-wolfe-dot-id-dot-au/buildkite-logs-parquet)
[![Go Report Card](https://goreportcard.com/badge/github.com/wolfeidau/buildkite-logs-parquet)](https://goreportcard.com/report/github.com/wolfeidau/buildkite-logs-parquet) 
[![Documentation](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/wolfeidau/buildkite-logs-parquet)


## Overview

Buildkite logs use a special format where each log entry is prefixed with an OSC sequence containing a timestamp:
```
\x1b_bk;t=1745322209921\x07~~~ Running global environment hook
```

This parser extracts the timestamps and content, providing both a Go library API and a command-line tool for processing these logs.

## Features

- **OSC Sequence Parsing**: Correctly handles Buildkite's `\x1b_bk;t=timestamp\x07content` format
- **Timestamp Extraction**: Converts millisecond timestamps to Go `time.Time` objects
- **ANSI Code Handling**: Optional stripping of ANSI escape sequences for clean text output
- **Content Classification**: Automatically identifies different types of log entries:
  - Commands (lines starting with `$`)
  - Section headers (lines starting with `~~~`, `---`, or `+++`)
- **Multiple Data Sources**: Local files and Buildkite API integration
- **Buildkite API**: Fetch logs directly from Buildkite jobs via REST API
- **Multiple Output Formats**: Text, JSON, and Parquet export
- **Filtering**: Filter logs by entry type (command, group)
- **Stream Processing**: Parse from any `io.Reader`
- **Group Tracking**: Automatically associate entries with build groups/sections
- **Parquet Export**: Efficient columnar storage for analytics and data processing
- **Parquet Query**: Fast querying of exported Parquet files with Apache Arrow Go v18
- **Parser Debugging**: Debug command for troubleshooting OSC sequence parsing issues

## CLI Usage

### Installation

**Using Make (recommended):**
```bash
# Build with tests and linting
make all

# Quick development build
make dev

# Build with specific version
make build VERSION=v1.2.3

# Other useful targets
make clean test lint help
```

**Manual build:**
```bash
make build
```

**Build a snapshot with [goreleaser](https://goreleaser.com/):**
```bash
goreleaser build --snapshot --clean --single-target
```

**Check version:**
```bash
./build/bklog version
# or
./build/bklog -v
# or  
./build/bklog --version
```

### Examples

#### Local File Processing

**Parse a log file with timestamps:**
```bash
./build/bklog parse -file buildkite.log -strip-ansi
```

**Output only commands:**
```bash
./build/bklog parse -file buildkite.log -filter command -strip-ansi
```

**Output only group headers:**
```bash
./build/bklog parse -file buildkite.log -filter group -strip-ansi
```

**JSON output:**
```bash
./build/bklog parse -file buildkite.log -json -strip-ansi
```

#### Buildkite API Integration

**Fetch logs directly from Buildkite API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -strip-ansi
```

**Export API logs to Parquet:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -parquet logs.parquet -summary
```

**Filter and export only commands from API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -filter command -json
```

**Show processing statistics:**
```bash
./build/bklog parse -file buildkite.log -summary -strip-ansi
```
Output:
```
--- Processing Summary ---
Bytes processed: 24.4 KB
Total entries: 212
Entries with timestamps: 212
Commands: 15
Sections: 13
Regular output: 184
```

**Show group/section information:**
```bash
./build/bklog -file buildkite.log -groups -strip-ansi | head -5
```
Output:
```
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] ~~~ Running global environment hook
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] $ /buildkite/agent/hooks/environment
[2025-04-22 21:43:29.948] [~~~ Running global pre-checkout hook] ~~~ Running global pre-checkout hook
[2025-04-22 21:43:29.949] [~~~ Running global pre-checkout hook] $ /buildkite/agent/hooks/pre-checkout
[2025-04-22 21:43:29.975] [~~~ Preparing working directory] ~~~ Preparing working directory
```

**Export to Parquet format:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet -summary
```
Output:
```
--- Processing Summary ---
Bytes processed: 24.4 KB
Total entries: 212
Entries with timestamps: 212
Commands: 15
Sections: 13
Regular output: 184
Exported 212 entries to output.parquet
```

**Export filtered data to Parquet:**
```bash
./build/bklog -file buildkite.log -parquet commands.parquet -filter command -summary
```
This exports only command entries to a smaller Parquet file for analysis.

### Querying Parquet Files

The CLI provides fast query operations on previously exported Parquet files:

**List all groups with statistics:**
```bash
./build/bklog query -file output.parquet -op list-groups
```
Output:
```
Groups found: 5

GROUP NAME                                ENTRIES COMMANDS          FIRST SEEN           LAST SEEN
------------------------------------------------------------------------------------------------------------
~~~ Running global environment hook             2        1 2025-04-22 21:43:29 2025-04-22 21:43:29
~~~ Running global pre-checkout hook            2        1 2025-04-22 21:43:29 2025-04-22 21:43:29
--- :package: Build job checkout dire...        2        1 2025-04-22 21:43:30 2025-04-22 21:43:30

--- Query Statistics ---
Total entries: 10
Matched entries: 10
Total groups: 5
Query time: 2.36 ms
```

**List all command entries:**
```bash
./build/bklog query -file output.parquet -op list-commands
```
Output:
```
Commands found: 15

[2025-04-22 21:43:29.921] [~~~ Running global environment hook] $ /buildkite/agent/hooks/environment
[2025-04-22 21:43:29.949] [~~~ Running global pre-checkout hook] $ /buildkite/agent/hooks/pre-checkout
[2025-04-22 21:43:29.975] [~~~ Preparing working directory] $ cd /buildkite/builds/g01mvtp4g0vi2-1/test/bash-example
[2025-04-22 21:43:29.975] [~~~ Preparing working directory] $ git clone -v -- https://github.com/buildkite/bash-example.git .
[2025-04-22 21:43:30.349] [~~~ Preparing working directory] $ git clean -ffxdq
...

--- Command Query Statistics (Streaming) ---
Total entries: 212
Total commands: 15
Query time: 1.08 ms
```

**List first 5 commands in JSON format:**
```bash
./build/bklog query -file output.parquet -op list-commands -format json -limit 5
```

**List commands without statistics:**
```bash
./build/bklog query -file output.parquet -op list-commands -stats=false
```

**Filter entries by group pattern:**
```bash
./build/bklog query -file output.parquet -op by-group -group "environment"
```
Output:
```
Entries in group matching 'environment': 2

[2025-04-22 21:43:29.921] [GRP] ~~~ Running global environment hook
[2025-04-22 21:43:29.922] [CMD] $ /buildkite/agent/hooks/environment

--- Query Statistics ---
Total entries: 10
Matched entries: 2
Query time: 0.36 ms
```

**Search entries using regex patterns:**
```bash
./build/bklog query -file output.parquet -op search -pattern "git clone"
```
Output:
```
Matches found: 1

[2025-04-22 21:43:29.975] [~~~ Preparing working directory] MATCH: $ git clone -v -- https://github.com/buildkite/bash-example.git .

--- Search Statistics (Streaming) ---
Total entries: 212
Matches found: 1
Query time: 0.65 ms
```

**Search with context lines (ripgrep-style):**
```bash
./build/bklog query -file output.parquet -op search -pattern "error|failed" -C 3
```
Output:
```
Matches found: 2

[2025-04-22 21:43:30.690] [~~~ Running script] Running tests...
[2025-04-22 21:43:30.691] [~~~ Running script] Test suite started
[2025-04-22 21:43:30.692] [~~~ Running script] Running unit tests
[2025-04-22 21:43:30.693] [~~~ Running script] MATCH: Test failed: authentication error
[2025-04-22 21:43:30.694] [~~~ Running script] Cleaning up test files
[2025-04-22 21:43:30.695] [~~~ Running script] Test run completed
[2025-04-22 21:43:30.696] [~~~ Running script] Generating report
--
[2025-04-22 21:43:30.750] [~~~ Post-processing] Validating results
[2025-04-22 21:43:30.751] [~~~ Post-processing] Checking exit codes
[2025-04-22 21:43:30.752] [~~~ Post-processing] Build status: some tests failed
[2025-04-22 21:43:30.753] [~~~ Post-processing] MATCH: Build failed due to test failures
[2025-04-22 21:43:30.754] [~~~ Post-processing] Uploading logs
[2025-04-22 21:43:30.755] [~~~ Post-processing] Notifying team
[2025-04-22 21:43:30.756] [~~~ Post-processing] Cleanup completed
```

**Search with separate before/after context:**
```bash
./build/bklog query -file output.parquet -op search -pattern "npm install" -B 2 -A 5
```

**Case-sensitive search:**
```bash
./build/bklog query -file output.parquet -op search -pattern "ERROR" --case-sensitive
```

**Invert match (show non-matching lines):**
```bash
./build/bklog query -file output.parquet -op search -pattern "buildkite" --invert-match -limit 5
```

**Search with JSON output:**
```bash
./build/bklog query -file output.parquet -op search -pattern "git clone" -format json -C 1
```

**JSON output for programmatic use:**
```bash
./build/bklog query -file output.parquet -op list-groups -format json
```

**Query without statistics:**
```bash
./build/bklog query -file output.parquet -op list-groups -stats=false
```

**Query last 20 entries:**
```bash
./build/bklog query -file output.parquet -op tail -tail 20
```

**Query specific row position:**
```bash
./build/bklog query -file output.parquet -op seek -seek 100
```

**Limit query results:**
```bash
./build/bklog query -file output.parquet -op by-group -group "test" -limit 50
```

**Get file information:**
```bash
./build/bklog query -file output.parquet -op info
```

**Dump all entries from the file:**
```bash
./build/bklog query -file output.parquet -op dump
```

**Dump with limited entries:**
```bash
./build/bklog query -file output.parquet -op dump -limit 100
```

**Dump all entries as JSON:**
```bash
./build/bklog query -file output.parquet -op dump -format json
```

#### Buildkite API Integration

The query command now supports direct API integration, automatically downloading and caching logs from Buildkite:

**Query logs directly from Buildkite API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op list-groups
```

**Query commands directly from Buildkite API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op list-commands
```

**Query specific group from API logs:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op by-group -group "tests"
```

**Search API logs with regex patterns:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op search -pattern "error|failed" -C 2
```

**Search API logs with case sensitivity:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op search -pattern "ERROR" --case-sensitive
```

**Query last 10 entries from API logs:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op tail -tail 10
```

**Get file info for cached API logs:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op info
```

**Dump all entries from API logs:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog query -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -op dump
```

Logs are automatically downloaded and cached in `~/.bklog/` as `{org}-{pipeline}-{build}-{job}.parquet` files. Subsequent queries use the cached version unless the cache is manually cleared.

### Debugging Parser Issues

The CLI includes a debug command for troubleshooting parser corruption issues, especially useful when investigating problems with OSC sequence parsing:

**Debug parser behavior on specific lines:**
```bash
./build/bklog debug -file buildkite.log -start 17 -limit 5 -verbose
```
Output:
```
=== Debug Mode: parse ===
File: buildkite.log
Lines: 17-21

--- Line 17 ---
Timestamp: 2025-07-01 09:20:41.629 +1000 AEST (Unix: 1751321141)
Content: "remote: Counting objects:   0% (1/287)K_bk;t=1751321141629remote: Counting objects:   1% (3/287)K..."
Group: ""
RawLine length: 6619
IsCommand: false
IsGroup: false
```

**Show hex dump of corrupted lines:**
```bash
./build/bklog debug -file buildkite.log -mode hex -start 17 -limit 1
```
Output:
```
=== Debug Mode: hex ===
File: buildkite.log
Lines: 17-17

--- Line 17 ---
Length: 6619 bytes
00000000  1b 5f 62 6b 3b 74 3d 31  37 35 31 33 32 31 31 34  |._bk;t=175132114|
00000010  31 36 32 39 07 72 65 6d  6f 74 65 3a 20 43 6f 75  |1629.remote: Cou|
00000020  6e 74 69 6e 67 20 6f 62  6a 65 63 74 73 3a 20 20  |nting objects:  |
00000030  20 30 25 20 28 31 2f 32  38 37 29 1b 5b 4b 1b 5f  | 0% (1/287).[K._|
00000040  62 6b 3b 74 3d 31 37 35  31 33 32 31 31 34 31 36  |bk;t=17513211416|
```

**Show raw line content with line numbers:**
```bash
./build/bklog debug -file buildkite.log -mode lines -start 100 -limit 3
```
Output:
```
=== Debug Mode: lines ===
File: buildkite.log
Lines: 100-102

--- Line 100 ---
Raw: "\x1b_bk;t=1751321141985\aremote: Total 2113 (delta 1830), reused 2113 (delta 1830), pack-reused 0\r"
Length: 98

--- Line 101 ---
Raw: "\x1b_bk;t=1751321142039\aReceiving objects: 100% (2113/2113), 630.45 KiB | 630.00 KiB/s, done.\r"
Length: 102
```

**Debug with combined options:**
```bash
./build/bklog debug -file buildkite.log -start 50 -end 55 -verbose -raw -hex
```

This will show verbose parse information, raw line content, and hex dump for lines 50-55.

#### Debug Command Options
```bash
./build/bklog debug [options]
```

**Required:**
- `-file <path>`: Path to log file to debug (required)

**Range Options:**
- `-start <line>`: Start line number (1-based, default: 1)
- `-end <line>`: End line number (0 = start+limit or EOF, default: 0)
- `-limit <num>`: Number of lines to process (default: 10)

**Mode Options:**
- `-mode <mode>`: Debug mode: `parse`, `hex`, `lines` (default: `parse`)

**Display Options:**
- `-verbose`: Show detailed parsing information (default: false)
- `-raw`: Show raw line content (default: false)
- `-hex`: Show hex dump of each line (default: false)
- `-parsed`: Show parsed log entry (default: true)

#### Use Cases

**Investigating Parser Corruption:**
The debug command is particularly useful for investigating issues where the parser only handles the first OSC sequence per line but ignores subsequent ones, causing content corruption.

**Common Issues Debugged:**
- Multiple OSC sequences per line (e.g., progress updates)
- Malformed OSC sequences missing proper terminators
- ANSI escape sequences interfering with parsing
- Timestamp extraction failures
- Content/group association problems

**Example Workflow:**
```bash
# 1. Identify problematic lines in output
./build/bklog parse -file buildkite.log | grep -n "unexpected content"

# 2. Debug specific lines with verbose output
./build/bklog debug -file buildkite.log -start 142 -limit 1 -verbose

# 3. Examine raw bytes if needed
./build/bklog debug -file buildkite.log -start 142 -limit 1 -mode hex

# 4. Compare multiple lines to understand patterns
./build/bklog debug -file buildkite.log -start 140 -end 145 -raw
```

#### Real Examples Using Test Data

The repository includes test data files that you can use to try out the tail functionality:

**View last 5 entries from the test log:**
```bash
./build/bklog query -file ./testdata/bash-example.parquet -op tail -tail 5
```
Output:
```
[2025-04-22 21:43:32.739] [CMD] $ echo 'Tests passed!'
[2025-04-22 21:43:32.740] Tests passed!
[2025-04-22 21:43:32.740] [GRP] +++ End of Example tests
[2025-04-22 21:43:32.740] [CMD] $ buildkite-agent annotate --style success 'Build passed'
[2025-04-22 21:43:32.748] Annotation added
```

**View last 10 entries (default) with JSON output:**
```bash
./build/bklog query -file ./testdata/bash-example.parquet -op tail -format json
```

**Parse the raw log file and immediately query the last 3 entries:**
```bash
# First create a fresh parquet file from the raw log
./build/bklog parse -file ./testdata/bash-example.log -parquet temp.parquet

# Then query the last 3 entries
./build/bklog query -file temp.parquet -op tail -tail 3
```

**Combine with other operations - show file info then tail:**
```bash
# Get file statistics
./build/bklog query -file ./testdata/bash-example.parquet -op info

# Then view the last few entries
./build/bklog query -file ./testdata/bash-example.parquet -op tail -tail 7
```

**Dump all entries from the test file:**
```bash
./build/bklog query -file ./testdata/bash-example.parquet -op dump
```

**Dump first 10 entries as JSON:**
```bash
./build/bklog query -file ./testdata/bash-example.parquet -op dump -limit 10 -format json
```

### CLI Options

#### Parse Command
```bash
./build/bklog parse [options]
```

**Local File Options:**
- `-file <path>`: Path to Buildkite log file (use this OR API parameters below)

**Buildkite API Options:**
- `-org <slug>`: Buildkite organization slug (for API access)
- `-pipeline <slug>`: Buildkite pipeline slug (for API access)
- `-build <number>`: Buildkite build number or UUID (for API access)
- `-job <id>`: Buildkite job ID (for API access)

**Output Options:**
- `-json`: Output as JSON instead of text
- `-strip-ansi`: Remove ANSI escape sequences from output
- `-filter <type>`: Filter entries by type (`command`, `group`)
- `-summary`: Show processing summary at the end
- `-groups`: Show group/section information for each entry
- `-parquet <path>`: Export to Parquet file (e.g., output.parquet)

#### Query Command
```bash
./build/bklog query [options]
```

**Data Source Options (choose one):**
- `-file <path>`: Path to Parquet log file (use this OR API parameters below)

**Buildkite API Options:**
- `-org <slug>`: Buildkite organization slug (for API access)
- `-pipeline <slug>`: Buildkite pipeline slug (for API access)
- `-build <number>`: Buildkite build number or UUID (for API access)
- `-job <id>`: Buildkite job ID (for API access)

**Query Options:**
- `-op <operation>`: Query operation (`list-groups`, `list-commands`, `by-group`, `search`, `info`, `tail`, `seek`, `dump`) (default: `list-groups`)
- `-group <pattern>`: Group name pattern to filter by (for `by-group` operation)
- `-format <format>`: Output format (`text`, `json`) (default: `text`)
- `-stats`: Show query statistics (default: `true`)
- `-limit <number>`: Limit number of entries returned (0 = no limit, enables early termination)
- `-tail <number>`: Number of lines to show from end (for `tail` operation, default: 10)
- `-seek <row>`: Row number to seek to (0-based, for `seek` operation)

**Search Options:**
- `-pattern <regex>`: Regex pattern to search for (for `search` operation)
- `-A <num>`: Show NUM lines after each match (ripgrep-style)
- `-B <num>`: Show NUM lines before each match (ripgrep-style)
- `-C <num>`: Show NUM lines before and after each match (ripgrep-style)
- `-case-sensitive`: Enable case-sensitive search (default: case-insensitive)
- `-invert-match`: Show non-matching lines instead of matching ones

#### Debug Command
```bash
./build/bklog debug [options]
```

**Required:**
- `-file <path>`: Path to log file to debug (required)

**Range Options:**
- `-start <line>`: Start line number (1-based, default: 1)
- `-end <line>`: End line number (0 = start+limit or EOF, default: 0)  
- `-limit <num>`: Number of lines to process (default: 10)

**Mode Options:**
- `-mode <mode>`: Debug mode: `parse`, `hex`, `lines` (default: `parse`)

**Display Options:**
- `-verbose`: Show detailed parsing information (default: false)
- `-raw`: Show raw line content (default: false)
- `-hex`: Show hex dump of each line (default: false)
- `-parsed`: Show parsed log entry (default: true)

**Note:** For API usage, set `BUILDKITE_API_TOKEN` environment variable. Logs are automatically downloaded and cached in `~/.bklog/`.

## Log Entry Types


### Commands
Lines that represent shell commands being executed:
```
[2025-04-22 21:43:29.975] $ git clone -v -- https://github.com/buildkite/bash-example.git .
```

### Groups
Headers that mark different phases of the build (collapsible in Buildkite UI):
```
[2025-04-22 21:43:29.921] ~~~ Running global environment hook
[2025-04-22 21:43:30.694] --- :package: Build job checkout directory
[2025-04-22 21:43:30.699] +++ :hammer: Example tests
```

### Groups/Sections

The parser automatically tracks which section or group each log entry belongs to:

```
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] ~~~ Running global environment hook
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] $ /buildkite/agent/hooks/environment
[2025-04-22 21:43:29.948] [~~~ Running global pre-checkout hook] ~~~ Running global pre-checkout hook
```

Each entry is automatically associated with the most recent group header (`~~~`, `---`, or `+++`). This allows you to:
- **Group related log entries** by build phase
- **Filter logs by group** for focused analysis  
- **Understand build structure** and timing relationships
- **Export structured data** with group context preserved

## Parquet Export

The parser can export log entries to [Apache Parquet](https://parquet.apache.org/) format using the official [Apache Arrow Go](https://github.com/apache/arrow/tree/main/go) implementation for efficient storage and analysis:

### Benefits of Parquet Format

- **Columnar storage**: Efficient compression and query performance
- **Schema preservation**: Maintains data types and structure
- **Analytics ready**: Compatible with Pandas, Apache Spark, DuckDB, and other data tools
- **Compact size**: Typically 70-90% smaller than JSON for log data
- **Fast queries**: Optimized for analytical workloads and filtering

### Parquet Schema

The exported Parquet files contain the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | int64 | Unix timestamp in milliseconds |
| `content` | string | Log content after OSC sequence |
| `group` | string | Current build group/section |
| `flags` | int32 | Bitwise flags field (HasTimestamp=1, IsCommand=2, IsGroup=4) |

### Flags Field

The `flags` column uses bitwise operations to efficiently store multiple boolean properties:

| Flag | Bit Position | Value | Description |
|------|-------------|--------|-------------|
| `HasTimestamp` | 0 | 1 | Entry has a valid timestamp |
| `IsCommand` | 1 | 2 | Entry is a shell command |
| `IsGroup` | 2 | 4 | Entry is a group header |

### Usage Examples

**Basic export:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet
```

**Export with filtering:**
```bash
./build/bklog -file buildkite.log -parquet commands.parquet -filter command
```

**Export with streaming processing:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet -summary
```
This uses the modern `iter.Seq2[*LogEntry, error]` iterator pattern for memory-efficient processing.

## API Reference

### Types

```go
type LogEntry struct {
    Timestamp time.Time  // Parsed timestamp (zero if no timestamp)
    Content   string     // Log content after OSC sequence
    RawLine   []byte     // Original raw log line as bytes
    Group     string     // Current section/group this entry belongs to
}

type Parser struct {
    // Internal regex patterns
}
```

### Methods

#### Parser Methods
```go
// Create a new parser
func NewParser() *Parser

// Parse a single log line
func (p *Parser) ParseLine(line string) (*LogEntry, error)

// Create Go 1.23+ iter.Seq2 iterator with proper error handling (streaming approach)
func (p *Parser) All(reader io.Reader) iter.Seq2[*LogEntry, error]

// Strip ANSI escape sequences
func (p *Parser) StripANSI(content string) string
```


#### LogEntry Methods
```go
func (entry *LogEntry) HasTimestamp() bool
func (entry *LogEntry) CleanContent() string  // Content with ANSI stripped
func (entry *LogEntry) IsCommand() bool
func (entry *LogEntry) IsGroup() bool         // Check if entry is a group header (~~~, ---, +++)
func (entry *LogEntry) IsSection() bool       // Deprecated: use IsGroup() instead
```

#### Parquet Export Functions
```go
// Export using iter.Seq2 streaming iterator
func ExportSeq2ToParquet(seq iter.Seq2[*LogEntry, error], filename string) error

// Export using iter.Seq2 with filtering
func ExportSeq2ToParquetWithFilter(seq iter.Seq2[*LogEntry, error], filename string, filterFunc func(*LogEntry) bool) error

// Create a new Parquet writer for streaming
func NewParquetWriter(file *os.File) *ParquetWriter

// Write a batch of entries to Parquet
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error

// Close the Parquet writer
func (pw *ParquetWriter) Close() error
```

#### Parquet Query Functions
```go
// Create a new Parquet reader
func NewParquetReader(filename string) *ParquetReader

// Stream entries from a Parquet file
func ReadParquetFileIter(filename string) iter.Seq2[ParquetLogEntry, error]

// Filter streaming entries by group pattern (case-insensitive)
func FilterByGroupIter(entries iter.Seq2[ParquetLogEntry, error], groupPattern string) iter.Seq2[ParquetLogEntry, error]
```

#### ParquetReader Methods
```go
// Stream all log entries from the Parquet file
func (pr *ParquetReader) ReadEntriesIter() iter.Seq2[ParquetLogEntry, error]

// Stream entries filtered by group pattern
func (pr *ParquetReader) FilterByGroupIter(groupPattern string) iter.Seq2[ParquetLogEntry, error]
```

#### Query Result Types
```go
type ParquetLogEntry struct {
    Timestamp   int64    `json:"timestamp"`    // Unix timestamp in milliseconds
    Content     string   `json:"content"`      // Log content
    Group       string   `json:"group"`        // Associated group/section
    Flags       LogFlags `json:"flags"`        // Bitwise flags (HasTimestamp=1, IsCommand=2, IsGroup=4)
}

// Backward-compatible methods
func (entry *ParquetLogEntry) HasTime() bool      // Returns Flags.HasTimestamp()
func (entry *ParquetLogEntry) IsCommand() bool    // Returns Flags.IsCommand()
func (entry *ParquetLogEntry) IsGroup() bool      // Returns Flags.IsGroup()

type LogFlags int32

// Bitwise flag operations
func (lf LogFlags) Has(flag LogFlag) bool         // Check if flag is set
func (lf *LogFlags) Set(flag LogFlag)             // Set flag
func (lf *LogFlags) Clear(flag LogFlag)           // Clear flag
func (lf *LogFlags) Toggle(flag LogFlag)          // Toggle flag

// Convenience methods
func (lf LogFlags) HasTimestamp() bool            // Check HasTimestamp flag
func (lf LogFlags) IsCommand() bool               // Check IsCommand flag  
func (lf LogFlags) IsGroup() bool                 // Check IsGroup flag

type GroupInfo struct {
    Name       string    `json:"name"`          // Group/section name
    EntryCount int       `json:"entry_count"`   // Number of entries in group
    FirstSeen  time.Time `json:"first_seen"`    // Timestamp of first entry
    LastSeen   time.Time `json:"last_seen"`     // Timestamp of last entry
    Commands   int       `json:"commands"`      // Number of command entries

}

```

## Performance

### Benchmarks

The parser includes comprehensive benchmarks to measure performance. Run them with:

```bash
go test -bench=. -benchmem
```

#### Key Results (Apple M3 Pro)

**Single Line Parsing (Byte-based):**
- OSC sequence with timestamp: ~64 ns/op, 192 B/op, 3 allocs/op
- Regular line (no timestamp): ~29 ns/op, 128 B/op, 2 allocs/op
- ANSI-heavy line: ~68 ns/op, 224 B/op, 3 allocs/op


**Memory Usage (10,000 lines):**
- **Seq2 Streaming Iterator**: ~3.5 MB allocated, 64,006 allocations
- **Constant memory footprint** regardless of file size

**Streaming Throughput:**
- **100 lines**: ~51,000 ops/sec
- **1,000 lines**: ~5,200 ops/sec
- **10,000 lines**: ~510 ops/sec
- **100,000 lines**: ~54 ops/sec

**ANSI Stripping**: ~7.7M ops/sec, 160 B/op, 2 allocs/op

**Parquet Export Performance (1,000 lines, Apache Arrow):**
- **Seq2 streaming export**: ~1,100 ops/sec, 1.2 MB allocated

**Content Classification Performance (1,000 entries):**
- **IsCommand()**: ~15,000 ops/sec, 84 KB allocated
- **IsGroup()**: ~14,000 ops/sec, 84 KB allocated

- **CleanContent()**: ~15,000 ops/sec, 84 KB allocated

**Parquet Streaming Query Performance (Apache Arrow Go v18):**
- **ReadEntriesIter**: Constant memory usage, ~5,700 entries/sec
- **FilterByGroupIter**: Early termination support, ~5,700 entries/sec
- **Memory-efficient**: Processes files of any size with constant memory footprint

**Streaming Query Scalability:**
- **Constant memory usage** regardless of file size
- **Early termination support** for partial processing
- **Linear processing time** scales with data size
- **No memory allocation growth** for large files

### Performance Improvements

**Byte-based Parser vs Regex:**
- **10x faster** OSC sequence parsing (~46ns vs ~477ns)
- **10x faster** ANSI stripping (~127ns vs ~1311ns)  
- **Fewer allocations** (2 vs 5 for ANSI stripping)
- **Better memory efficiency** for complex lines

**Streaming Memory Efficiency:**
- **Constant memory footprint** regardless of file size
- **True streaming processing** for files of any size
- **Early termination** capability with immediate resource cleanup
- **Memory-safe** processing of multi-gigabyte files

## Testing

Run the test suite:
```bash
go test -v
```

Run benchmarks:
```bash
go test -bench=. -benchmem
```

The tests cover:
- OSC sequence parsing
- Timestamp extraction
- ANSI code stripping
- Content classification
- Stream processing
- Iterator functionality
- Memory usage patterns

## Acknowledgments

This library was developed with assistance from Claude (Anthropic) for parsing, query functionality, and performance optimization.

## License

This project is licensed under the MIT License.