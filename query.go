package buildkitelogs

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetLogEntry represents a log entry read from a Parquet file
type ParquetLogEntry struct {
	Timestamp int64  `json:"timestamp"`
	Content   string `json:"content"`
	Group     string `json:"group"`
	HasTime   bool   `json:"has_timestamp"`
	IsCommand bool   `json:"is_command"`
	IsGroup   bool   `json:"is_group"`
}

// GroupInfo contains statistical information about a log group
type GroupInfo struct {
	Name       string    `json:"name"`
	EntryCount int       `json:"entry_count"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	Commands   int       `json:"commands"`
}

// SearchOptions configures regex search behavior
type SearchOptions struct {
	Pattern       string // Regex pattern to search for
	CaseSensitive bool   // Enable case-sensitive matching
	InvertMatch   bool   // Show non-matching lines
	BeforeContext int    // Lines to show before match
	AfterContext  int    // Lines to show after match
	Context       int    // Lines to show before and after (overrides BeforeContext/AfterContext)
}

// SearchResult represents a match with context lines
type SearchResult struct {
	Match         ParquetLogEntry   `json:"match"`
	BeforeContext []ParquetLogEntry `json:"before_context,omitempty"`
	AfterContext  []ParquetLogEntry `json:"after_context,omitempty"`
	LineNumber    int64             `json:"line_number"`
}

// QueryStats contains performance and result statistics for queries
type QueryStats struct {
	TotalEntries   int     `json:"total_entries"`
	MatchedEntries int     `json:"matched_entries"`
	TotalGroups    int     `json:"total_groups"`
	QueryTime      float64 `json:"query_time_ms"`
}

// QueryResult holds the results of a query operation
type QueryResult struct {
	Groups  []GroupInfo       `json:"groups,omitempty"`
	Entries []ParquetLogEntry `json:"entries,omitempty"`
	Stats   QueryStats        `json:"stats,omitempty"`
}

// ParquetFileInfo contains metadata about a Parquet file
type ParquetFileInfo struct {
	RowCount     int64 `json:"row_count"`
	ColumnCount  int   `json:"column_count"`
	FileSize     int64 `json:"file_size_bytes"`
	NumRowGroups int   `json:"num_row_groups"`
}

// ParquetReader provides functionality to read and query Parquet log files
type ParquetReader struct {
	filename string
}

// NewParquetReader creates a new ParquetReader for the specified file
func NewParquetReader(filename string) *ParquetReader {
	return &ParquetReader{
		filename: filename,
	}
}

// ReadEntriesIter returns an iterator over log entries from the Parquet file
func (pr *ParquetReader) ReadEntriesIter() iter.Seq2[ParquetLogEntry, error] {
	return readParquetFileIter(pr.filename)
}

// FilterByGroupIter returns an iterator over entries that belong to groups matching the specified name pattern
func (pr *ParquetReader) FilterByGroupIter(groupPattern string) iter.Seq2[ParquetLogEntry, error] {
	return FilterByGroupIter(pr.ReadEntriesIter(), groupPattern)
}

// SeekToRow returns an iterator starting from the specified row number (0-based)
func (pr *ParquetReader) SeekToRow(startRow int64) iter.Seq2[ParquetLogEntry, error] {
	return readParquetFileFromRowIter(pr.filename, startRow)
}

// GetFileInfo returns metadata about the Parquet file
func (pr *ParquetReader) GetFileInfo() (*ParquetFileInfo, error) {
	return getParquetFileInfo(pr.filename)
}

// SearchEntriesIter returns an iterator over search results with context
func (pr *ParquetReader) SearchEntriesIter(options SearchOptions) iter.Seq2[SearchResult, error] {
	return searchParquetFileIter(pr.filename, options)
}

// ReadParquetFileIter is a convenience function to get an iterator over entries from a Parquet file
func ReadParquetFileIter(filename string) iter.Seq2[ParquetLogEntry, error] {
	return readParquetFileIter(filename)
}

// readParquetFileIter reads a Parquet file and returns an iterator over log entries using streaming
func readParquetFileIter(filename string) iter.Seq2[ParquetLogEntry, error] {
	return readParquetFileStreamingIter(filename, 5000) // Use 5000 as default batch size
}

// readParquetFileStreamingIter reads a Parquet file using GetRecordReader for true streaming
func readParquetFileStreamingIter(filename string, batchSize int64) iter.Seq2[ParquetLogEntry, error] {
	return func(yield func(ParquetLogEntry, error) bool) {
		// Resource management with proper cleanup order
		resources := make([]func(), 0)
		defer func() {
			for i := len(resources) - 1; i >= 0; i-- {
				resources[i]()
			}
		}()

		// Open the Parquet file
		osFile, err := os.Open(filename)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to open file: %w", err))
			return
		}
		resources = append(resources, func() { _ = osFile.Close() })

		// Create a memory pool
		pool := memory.NewGoAllocator()

		// Create a Parquet file reader using Arrow v18 API
		pf, err := file.NewParquetReader(osFile)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to open parquet file: %w", err))
			return
		}
		resources = append(resources, func() { _ = pf.Close() })

		// Create an Arrow file reader with streaming configuration
		ctx := context.Background()
		arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
			BatchSize: batchSize, // Configure batch size for streaming
		}, pool)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to create arrow reader: %w", err))
			return
		}

		// Get record reader for true streaming (all columns, all row groups)
		recordReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to create record reader: %w", err))
			return
		}
		resources = append(resources, func() { recordReader.Release() })

		// Get schema from the first record peek or metadata
		var columnIndices *columnMapping

		// Stream records in batches
		for {
			record, err := recordReader.Read()
			if err != nil {
				if err == io.EOF {
					break // Normal end of file
				}
				yield(ParquetLogEntry{}, fmt.Errorf("error reading record: %w", err))
				return
			}

			// Initialize column mapping on first record
			if columnIndices == nil {
				columnIndices, err = mapColumns(record.Schema())
				if err != nil {
					record.Release()
					yield(ParquetLogEntry{}, err)
					return
				}
			}

			// Process record batch with immediate cleanup
			shouldContinue := func() bool {
				defer record.Release()

				// Convert record to entries using streaming iterator
				for entry, err := range convertRecordToEntriesIterStreaming(record, columnIndices) {
					if !yield(entry, err) {
						return false
					}
				}
				return true
			}()

			if !shouldContinue {
				return
			}
		}
	}
}

// columnMapping holds column indices for efficient access
type columnMapping struct {
	timestampIdx, contentIdx, groupIdx, hasTimeIdx, isCmdIdx, isGroupIdx int
}

// mapColumns maps column names to indices from schema
func mapColumns(schema *arrow.Schema) (*columnMapping, error) {
	mapping := &columnMapping{
		timestampIdx: -1, contentIdx: -1, groupIdx: -1, hasTimeIdx: -1,
		isCmdIdx: -1, isGroupIdx: -1,
	}

	for i, field := range schema.Fields() {
		switch field.Name {
		case "timestamp":
			mapping.timestampIdx = i
		case "content":
			mapping.contentIdx = i
		case "group":
			mapping.groupIdx = i
		case "has_timestamp":
			mapping.hasTimeIdx = i
		case "is_command":
			mapping.isCmdIdx = i
		case "is_group":
			mapping.isGroupIdx = i

		}
	}

	if mapping.timestampIdx == -1 || mapping.contentIdx == -1 {
		return nil, fmt.Errorf("required columns 'timestamp' and 'content' not found")
	}

	return mapping, nil
}

// convertRecordToEntriesIterStreaming converts an Arrow record to an iterator over ParquetLogEntry with column mapping
func convertRecordToEntriesIterStreaming(record arrow.Record, mapping *columnMapping) iter.Seq2[ParquetLogEntry, error] {
	return func(yield func(ParquetLogEntry, error) bool) {
		numRows := int(record.NumRows())

		// Get column arrays
		timestampCol := record.Column(mapping.timestampIdx)
		contentCol := record.Column(mapping.contentIdx)

		var groupCol, hasTimeCol, isCmdCol, isGroupCol arrow.Array
		if mapping.groupIdx >= 0 {
			groupCol = record.Column(mapping.groupIdx)
		}
		if mapping.hasTimeIdx >= 0 {
			hasTimeCol = record.Column(mapping.hasTimeIdx)
		}
		if mapping.isCmdIdx >= 0 {
			isCmdCol = record.Column(mapping.isCmdIdx)
		}
		if mapping.isGroupIdx >= 0 {
			isGroupCol = record.Column(mapping.isGroupIdx)
		}

		// Convert each row
		for i := 0; i < numRows; i++ {
			entry := ParquetLogEntry{}

			// Timestamp (required)
			if timestampCol.IsNull(i) {
				entry.Timestamp = 0
			} else {
				switch ts := timestampCol.(type) {
				case *array.Int64:
					entry.Timestamp = ts.Value(i)
				default:
					yield(ParquetLogEntry{}, fmt.Errorf("unexpected timestamp column type: %T", timestampCol))
					return
				}
			}

			// Content (required)
			if contentCol.IsNull(i) {
				entry.Content = ""
			} else {
				switch content := contentCol.(type) {
				case *array.String:
					entry.Content = content.Value(i)
				case *array.Binary:
					entry.Content = string(content.Value(i))
				default:
					yield(ParquetLogEntry{}, fmt.Errorf("unexpected content column type: %T", contentCol))
					return
				}
			}

			// Group (optional)
			if groupCol != nil && !groupCol.IsNull(i) {
				switch group := groupCol.(type) {
				case *array.String:
					entry.Group = group.Value(i)
				case *array.Binary:
					entry.Group = string(group.Value(i))
				}
			}

			// Boolean fields (optional)
			if hasTimeCol != nil && !hasTimeCol.IsNull(i) {
				if boolCol, ok := hasTimeCol.(*array.Boolean); ok {
					entry.HasTime = boolCol.Value(i)
				}
			}
			if isCmdCol != nil && !isCmdCol.IsNull(i) {
				if boolCol, ok := isCmdCol.(*array.Boolean); ok {
					entry.IsCommand = boolCol.Value(i)
				}
			}
			if isGroupCol != nil && !isGroupCol.IsNull(i) {
				if boolCol, ok := isGroupCol.(*array.Boolean); ok {
					entry.IsGroup = boolCol.Value(i)
				}
			}

			if !yield(entry, nil) {
				return
			}
		}
	}
}

// FilterByGroupIter returns an iterator over entries that belong to groups matching the specified pattern
func FilterByGroupIter(entries iter.Seq2[ParquetLogEntry, error], groupPattern string) iter.Seq2[ParquetLogEntry, error] {
	return func(yield func(ParquetLogEntry, error) bool) {
		for entry, err := range entries {
			if err != nil {
				if !yield(ParquetLogEntry{}, err) {
					return
				}
				continue
			}

			entryGroup := entry.Group
			if entryGroup == "" {
				entryGroup = "<no group>"
			}

			if strings.Contains(strings.ToLower(entryGroup), strings.ToLower(groupPattern)) {
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}

// getParquetFileInfo returns metadata about the Parquet file
func getParquetFileInfo(filename string) (*ParquetFileInfo, error) {
	// Open the file to get file size
	osFile, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer osFile.Close()

	// Get file size
	fileInfo, err := osFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// Create Parquet file reader
	pf, err := file.NewParquetReader(osFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer pf.Close()

	// Get metadata
	metadata := pf.MetaData()

	// Count columns
	columnCount := 0
	for range metadata.Schema.Columns() {
		columnCount++
	}

	info := &ParquetFileInfo{
		RowCount:     metadata.GetNumRows(),
		ColumnCount:  columnCount,
		FileSize:     fileInfo.Size(),
		NumRowGroups: metadata.NumRowGroups(),
	}

	return info, nil
}

// readParquetFileFromRowIter reads a Parquet file starting from a specific row
func readParquetFileFromRowIter(filename string, startRow int64) iter.Seq2[ParquetLogEntry, error] {
	return func(yield func(ParquetLogEntry, error) bool) {
		// Resource management with proper cleanup order
		resources := make([]func(), 0)
		defer func() {
			for i := len(resources) - 1; i >= 0; i-- {
				resources[i]()
			}
		}()

		// Open the Parquet file
		osFile, err := os.Open(filename)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to open file: %w", err))
			return
		}
		resources = append(resources, func() { _ = osFile.Close() })

		// Create a memory pool
		pool := memory.NewGoAllocator()

		// Create a Parquet file reader using Arrow v18 API
		pf, err := file.NewParquetReader(osFile)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to open parquet file: %w", err))
			return
		}
		resources = append(resources, func() { _ = pf.Close() })

		// Check if startRow is valid
		totalRows := pf.MetaData().GetNumRows()
		if startRow >= totalRows {
			yield(ParquetLogEntry{}, fmt.Errorf("start row %d is beyond file bounds (total rows: %d)", startRow, totalRows))
			return
		}

		// Create an Arrow file reader
		ctx := context.Background()
		arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
			BatchSize: 5000, // Default batch size
		}, pool)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to create arrow reader: %w", err))
			return
		}

		// Get record reader for all columns and row groups
		recordReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
		if err != nil {
			yield(ParquetLogEntry{}, fmt.Errorf("failed to create record reader: %w", err))
			return
		}
		resources = append(resources, func() { recordReader.Release() })

		// Use Arrow's built-in SeekToRow for efficient seeking
		if startRow > 0 {
			if err := recordReader.SeekToRow(startRow); err != nil {
				yield(ParquetLogEntry{}, fmt.Errorf("failed to seek to row %d: %w", startRow, err))
				return
			}
		}

		// Get schema for column mapping
		var columnIndices *columnMapping

		// Stream records in batches starting from the seek position
		for {
			record, err := recordReader.Read()
			if err != nil {
				if err == io.EOF {
					break // Normal end of file
				}
				yield(ParquetLogEntry{}, fmt.Errorf("error reading record: %w", err))
				return
			}

			// Initialize column mapping on first record
			if columnIndices == nil {
				columnIndices, err = mapColumns(record.Schema())
				if err != nil {
					record.Release()
					yield(ParquetLogEntry{}, err)
					return
				}
			}

			// Process all entries in this record batch (no manual offset needed)
			shouldContinue := func() bool {
				defer record.Release()

				// Convert record to entries using standard streaming iterator
				for entry, err := range convertRecordToEntriesIterStreaming(record, columnIndices) {
					if !yield(entry, err) {
						return false
					}
				}
				return true
			}()

			if !shouldContinue {
				return
			}
		}
	}
}

// searchParquetFileIter implements streaming search with context
func searchParquetFileIter(filename string, options SearchOptions) iter.Seq2[SearchResult, error] {
	return func(yield func(SearchResult, error) bool) {
		// Compile regex pattern
		regex, err := compileRegexPattern(options.Pattern, options.CaseSensitive)
		if err != nil {
			yield(SearchResult{}, fmt.Errorf("invalid regex: %w", err))
			return
		}

		// Determine context lines
		beforeContext := options.BeforeContext
		afterContext := options.AfterContext
		if options.Context > 0 {
			beforeContext = options.Context
			afterContext = options.Context
		}

		// Stream entries and perform search with context buffering
		var beforeBuffer []ParquetLogEntry
		var afterCollecting int
		var currentResult *SearchResult
		totalEntries := int64(0)

		for entry, err := range readParquetFileIter(filename) {
			if err != nil {
				yield(SearchResult{}, err)
				return
			}

			totalEntries++

			// Handle after-context collection
			if afterCollecting > 0 && currentResult != nil {
				currentResult.AfterContext = append(currentResult.AfterContext, entry)
				afterCollecting--
				if afterCollecting == 0 {
					// Yield the completed result
					if !yield(*currentResult, nil) {
						return
					}
					currentResult = nil
				}
			}

			// Test match
			isMatch := regex.MatchString(entry.Content)
			if options.InvertMatch {
				isMatch = !isMatch
			}

			if isMatch {
				result := SearchResult{
					Match:         entry,
					BeforeContext: make([]ParquetLogEntry, len(beforeBuffer)),
					AfterContext:  make([]ParquetLogEntry, 0, afterContext),
					LineNumber:    totalEntries - 1,
				}
				copy(result.BeforeContext, beforeBuffer)

				// If no after-context needed, yield immediately
				if afterContext == 0 {
					if !yield(result, nil) {
						return
					}
				} else {
					// Set up after-context collection
					currentResult = &result
					afterCollecting = afterContext
				}

				// Clear before buffer after match
				beforeBuffer = beforeBuffer[:0]
			} else if beforeContext > 0 {
				// Maintain rolling before-context buffer
				if len(beforeBuffer) >= beforeContext {
					beforeBuffer = beforeBuffer[1:]
				}
				beforeBuffer = append(beforeBuffer, entry)
			}
		}

		// If we have a pending result waiting for after-context, yield it
		if currentResult != nil {
			yield(*currentResult, nil)
		}
	}
}

// compileRegexPattern compiles a regex pattern with optional case sensitivity
func compileRegexPattern(pattern string, caseSensitive bool) (*regexp.Regexp, error) {
	if !caseSensitive {
		pattern = "(?i)" + pattern
	}
	return regexp.Compile(pattern)
}
