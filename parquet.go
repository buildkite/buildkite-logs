package buildkitelogs

import (
	"fmt"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func createNewFileWriter(schema *arrow.Schema, file *os.File, pool memory.Allocator) (*pqarrow.FileWriter, error) {
	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(schema, file,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3),
			parquet.WithSortingColumns([]parquet.SortingColumn{
				{ColumnIdx: 0, Descending: false, NullsFirst: true}, // Timestamp
				{ColumnIdx: 2, Descending: false, NullsFirst: true}, // Group
			}),
		),
		pqarrow.NewArrowWriterProperties(
			pqarrow.WithAllocator(pool),
			pqarrow.WithCoerceTimestamps(arrow.Millisecond),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	return writer, nil
}

// createArrowSchema creates the Arrow schema for log entries
func createArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}, Nullable: false},
		{Name: "group", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}, Nullable: false},
		{Name: "has_timestamp", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_command", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_group", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_progress", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
}

// createRecordFromEntries creates an Arrow record from log entries using the writer's builders
func (pw *ParquetWriter) createRecordFromEntries(entries []*LogEntry) (arrow.Record, error) {
	// Reset dictionary builders for new batch (preserves dictionaries)
	pw.contentBuilder.ResetFull()
	pw.groupBuilder.ResetFull()

	// Reserve capacity
	numEntries := len(entries)
	pw.timestampBuilder.Resize(numEntries)
	pw.contentBuilder.Resize(numEntries)
	pw.groupBuilder.Resize(numEntries)
	pw.hasTimestampBuilder.Resize(numEntries)
	pw.isCommandBuilder.Resize(numEntries)
	pw.isGroupBuilder.Resize(numEntries)
	pw.isProgressBuilder.Resize(numEntries)

	// Populate arrays
	for _, entry := range entries {
		pw.timestampBuilder.Append(entry.Timestamp.UnixMilli())
		if err := pw.contentBuilder.Append([]byte(entry.Content)); err != nil {
			return nil, err
		}
		if err := pw.groupBuilder.Append([]byte(entry.Group)); err != nil {
			return nil, err
		}
		pw.hasTimestampBuilder.Append(entry.HasTimestamp())
		pw.isCommandBuilder.Append(entry.IsCommand())
		pw.isGroupBuilder.Append(entry.IsGroup())
		pw.isProgressBuilder.Append(entry.IsProgress())
	}

	// Build arrays
	timestampArray := pw.timestampBuilder.NewArray()
	contentArray := pw.contentBuilder.NewArray()
	groupArray := pw.groupBuilder.NewArray()
	hasTimestampArray := pw.hasTimestampBuilder.NewArray()
	isCommandArray := pw.isCommandBuilder.NewArray()
	isGroupArray := pw.isGroupBuilder.NewArray()
	isProgressArray := pw.isProgressBuilder.NewArray()

	defer timestampArray.Release()
	defer contentArray.Release()
	defer groupArray.Release()
	defer hasTimestampArray.Release()
	defer isCommandArray.Release()
	defer isGroupArray.Release()
	defer isProgressArray.Release()

	// Create record
	return array.NewRecord(pw.schema, []arrow.Array{
		timestampArray,
		contentArray,
		groupArray,
		hasTimestampArray,
		isCommandArray,
		isGroupArray,
		isProgressArray,
	}, int64(numEntries)), nil
}

// ParquetWriter provides streaming Parquet writing capabilities
type ParquetWriter struct {
	file   *os.File
	writer *pqarrow.FileWriter
	pool   memory.Allocator
	schema *arrow.Schema

	// Persistent builders for dictionary encoding across batches
	timestampBuilder    *array.Int64Builder
	contentBuilder      *array.BinaryDictionaryBuilder
	groupBuilder        *array.BinaryDictionaryBuilder
	hasTimestampBuilder *array.BooleanBuilder
	isCommandBuilder    *array.BooleanBuilder
	isGroupBuilder      *array.BooleanBuilder
	isProgressBuilder   *array.BooleanBuilder
}

// NewParquetWriter creates a new Parquet writer for streaming
func NewParquetWriter(file *os.File) *ParquetWriter {
	pool := memory.NewGoAllocator()
	schema := createArrowSchema()

	writer, err := createNewFileWriter(schema, file, pool)
	if err != nil {
		return nil // In a real implementation, we'd want to return the error
	}

	return &ParquetWriter{
		file:   file,
		writer: writer,
		pool:   pool,
		schema: schema,

		// Initialize builders once for dictionary encoding across batches
		timestampBuilder:    array.NewInt64Builder(pool),
		contentBuilder:      array.NewDictionaryBuilder(pool, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}).(*array.BinaryDictionaryBuilder),
		groupBuilder:        array.NewDictionaryBuilder(pool, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}).(*array.BinaryDictionaryBuilder),
		hasTimestampBuilder: array.NewBooleanBuilder(pool),
		isCommandBuilder:    array.NewBooleanBuilder(pool),
		isGroupBuilder:      array.NewBooleanBuilder(pool),
		isProgressBuilder:   array.NewBooleanBuilder(pool),
	}
}

// WriteBatch writes a batch of log entries to the Parquet file
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	record, err := pw.createRecordFromEntries(entries)
	if err != nil {
		return err
	}
	defer record.Release()

	return pw.writer.Write(record)
}

// Close closes the Parquet writer
func (pw *ParquetWriter) Close() error {
	// Release all builders
	pw.timestampBuilder.Release()
	pw.contentBuilder.Release()
	pw.groupBuilder.Release()
	pw.hasTimestampBuilder.Release()
	pw.isCommandBuilder.Release()
	pw.isGroupBuilder.Release()
	pw.isProgressBuilder.Release()

	return pw.writer.Close()
}

// ExportSeq2ToParquet exports log entries using Go 1.23+ iter.Seq2 for efficient iteration
func ExportSeq2ToParquet(seq iter.Seq2[*LogEntry, error], filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Create writer
	writer := NewParquetWriter(file)
	if writer == nil {
		return fmt.Errorf("failed to create Parquet writer")
	}
	defer func() { _ = writer.Close() }()

	// Process entries in batches for memory efficiency
	const batchSize = 10000
	batch := make([]*LogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}

		batch = append(batch, entry)

		// Write batch when full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset slice
		}
	}

	// Write final batch
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

// ExportSeq2ToParquetWithFilter exports filtered log entries using iter.Seq2
func ExportSeq2ToParquetWithFilter(seq iter.Seq2[*LogEntry, error], filename string, filterFunc func(*LogEntry) bool) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Create writer
	writer := NewParquetWriter(file)
	if writer == nil {
		return fmt.Errorf("failed to create Parquet writer")
	}
	defer func() { _ = writer.Close() }()

	// Process entries in batches for memory efficiency
	const batchSize = 10000
	batch := make([]*LogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}

		// Apply filter if provided
		if filterFunc != nil && !filterFunc(entry) {
			continue
		}

		batch = append(batch, entry)

		// Write batch when full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset slice
		}
	}

	// Write final batch
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
		if err != nil {
			return err
		}
	}

	return nil
}
