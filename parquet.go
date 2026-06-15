package buildkitelogs

import (
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/buildkite/buildkite-logs/logparser"
)

func createNewFileWriter(schema *arrow.Schema, w io.Writer, pool memory.Allocator) (*pqarrow.FileWriter, error) {
	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(schema, w,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
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
		{Name: "content", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "group", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
}

// createRecord creates an Arrow record from log entries using the writer's builders
func (pw *ParquetWriter) createRecord(entries []*logparser.Entry) arrow.RecordBatch {
	numEntries := len(entries)
	pw.timestampBuilder.Resize(numEntries)
	pw.contentBuilder.Resize(numEntries)
	pw.groupBuilder.Resize(numEntries)
	pw.flagsBuilder.Resize(numEntries)

	for _, entry := range entries {
		pw.timestampBuilder.Append(entry.Timestamp.UnixMilli())
		pw.contentBuilder.Append(entry.Content)
		pw.groupBuilder.Append(entry.Group)
		pw.flagsBuilder.Append(int32(entry.ComputeFlags()))
	}

	timestampArray := pw.timestampBuilder.NewArray()
	contentArray := pw.contentBuilder.NewArray()
	groupArray := pw.groupBuilder.NewArray()
	flagsArray := pw.flagsBuilder.NewArray()

	defer timestampArray.Release()
	defer contentArray.Release()
	defer groupArray.Release()
	defer flagsArray.Release()

	return array.NewRecordBatch(pw.schema, []arrow.Array{
		timestampArray,
		contentArray,
		groupArray,
		flagsArray,
	}, int64(numEntries))
}

// ParquetWriter provides streaming Parquet writing capabilities
type ParquetWriter struct {
	writer *pqarrow.FileWriter
	pool   memory.Allocator
	schema *arrow.Schema

	// Persistent builders for string encoding
	timestampBuilder *array.Int64Builder
	contentBuilder   *array.StringBuilder
	groupBuilder     *array.StringBuilder
	flagsBuilder     *array.Int32Builder
}

// NewParquetWriter creates a new Parquet writer for streaming
func NewParquetWriter(file *os.File) (*ParquetWriter, error) {
	return NewParquetWriterForWriter(file)
}

// NewParquetWriterForWriter creates a new Parquet writer backed by any io.Writer.
func NewParquetWriterForWriter(w io.Writer) (*ParquetWriter, error) {
	return newParquetWriterWithPool(w, memory.NewGoAllocator())
}

// newParquetWriterWithPool creates a ParquetWriter using the provided allocator.
// Used in tests to inject a memory.NewCheckedAllocator for leak detection.
func newParquetWriterWithPool(w io.Writer, pool memory.Allocator) (*ParquetWriter, error) {
	schema := createArrowSchema()

	writer, err := createNewFileWriter(schema, w, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	return &ParquetWriter{
		writer: writer,
		pool:   pool,
		schema: schema,

		// Initialize builders for string encoding
		timestampBuilder: array.NewInt64Builder(pool),
		contentBuilder:   array.NewStringBuilder(pool),
		groupBuilder:     array.NewStringBuilder(pool),
		flagsBuilder:     array.NewInt32Builder(pool),
	}, nil
}

// WriteBatch writes a batch of log entries to the Parquet file
func (pw *ParquetWriter) WriteBatch(entries []*logparser.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	record := pw.createRecord(entries)
	defer record.Release()

	return pw.writer.Write(record)
}

// Close closes the Parquet writer
func (pw *ParquetWriter) Close() error {
	// Release all builders
	pw.timestampBuilder.Release()
	pw.contentBuilder.Release()
	pw.groupBuilder.Release()
	pw.flagsBuilder.Release()

	return pw.writer.Close()
}

// ExportSeq2ToParquet exports log entries using Go 1.23+ iter.Seq2 for efficient iteration
func ExportSeq2ToParquet(seq iter.Seq2[*logparser.Entry, error], filename string) error {
	_, err := ExportSeq2ToParquetWithFilterAndStats(seq, filename, nil)
	return err
}

// ExportSeq2ToParquetWithFilter exports filtered log entries using iter.Seq2
func ExportSeq2ToParquetWithFilter(seq iter.Seq2[*logparser.Entry, error], filename string, filterFunc func(*logparser.Entry) bool) error {
	_, err := ExportSeq2ToParquetWithFilterAndStats(seq, filename, filterFunc)
	return err
}

// ExportSeq2ToParquetWithFilterAndStats exports filtered log entries and returns the number of rows written.
func ExportSeq2ToParquetWithFilterAndStats(seq iter.Seq2[*logparser.Entry, error], filename string, filterFunc func(*logparser.Entry) bool) (int, error) {
	file, err := os.Create(filename) //nolint:gosec // caller-controlled path
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	return ExportSeq2ToParquetWriterWithFilter(seq, file, filterFunc)
}

// ExportSeq2ToParquetWriter exports log entries to any io.Writer.
func ExportSeq2ToParquetWriter(seq iter.Seq2[*logparser.Entry, error], w io.Writer) (int, error) {
	return ExportSeq2ToParquetWriterWithFilter(seq, w, nil)
}

// ExportSeq2ToParquetWriterWithFilter exports filtered log entries to any io.Writer.
func ExportSeq2ToParquetWriterWithFilter(seq iter.Seq2[*logparser.Entry, error], w io.Writer, filterFunc func(*logparser.Entry) bool) (int, error) {
	writer, err := NewParquetWriterForWriter(w)
	if err != nil {
		return 0, err
	}
	defer func() { _ = writer.Close() }()

	const batchSize = 1000
	batch := make([]*logparser.Entry, 0, batchSize)
	rows := 0

	for entry, err := range seq {
		if err != nil {
			return rows, fmt.Errorf("error during iteration: %w", err)
		}

		if filterFunc != nil && !filterFunc(entry) {
			continue
		}

		batch = append(batch, entry)
		rows++

		if len(batch) >= batchSize {
			if err := writer.WriteBatch(batch); err != nil {
				return rows, err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := writer.WriteBatch(batch); err != nil {
			return rows, err
		}
	}

	return rows, nil
}
