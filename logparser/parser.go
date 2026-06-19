package logparser

import (
	"bytes"
	"errors"
	"io"
	"iter"
	"strconv"
	"time"
)

var oscStart = []byte{0x1b, '_', 'b', 'k', ';', 't', '='}

// Parser handles Buildkite log parsing and group tracking.
type Parser struct {
	opts         Options
	currentGroup string
}

func New(options ...Option) *Parser {
	return newParserWithOptions(optionsFrom(options...))
}

func newParserWithOptions(opts Options) *Parser {
	return &Parser{
		opts: normalizeOptions(opts),
	}
}

func (p *Parser) ParseLine(line string) (*Entry, error) {
	return p.ParseLineBytes([]byte(line), Line{})
}

func (p *Parser) ParseLineBytes(line []byte, meta Line) (*Entry, error) {
	if meta.Number == 0 {
		meta.Number = 1
	}

	entry, err := parseLine(line, meta, p.opts)
	if err != nil {
		return nil, err
	}

	if entry.IsGroup() {
		p.currentGroup = entry.Content
	}
	entry.Group = p.currentGroup

	return entry, nil
}

// All returns an iterator over all parsed log entries. Each iteration has
// isolated group state so a parser can be reused safely.
func (p *Parser) All(reader io.Reader) iter.Seq2[*Entry, error] {
	return func(yield func(*Entry, error) bool) {
		localParser := newParserWithOptions(p.opts)
		lineReader := newLineReaderWithOptions(reader, p.opts)

		for {
			line, err := lineReader.Next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				_ = yield(nil, err)
				return
			}

			entry, err := localParser.ParseLineBytes(line.Bytes, line)
			if err != nil {
				_ = yield(nil, err)
				return
			}
			if !yield(entry, nil) {
				return
			}
		}
	}
}

func parseLine(line []byte, meta Line, opts Options) (*Entry, error) {
	raw := append([]byte(nil), line...)

	if len(line) < len(oscStart)+2 || !hasOSCStart(line) {
		return &Entry{
			Content: string(line),
			RawLine: raw,
		}, nil
	}

	timestampStart := len(oscStart)
	timestampEnd := bytes.IndexByte(line[timestampStart:], 0x07)
	if timestampEnd == -1 {
		return &Entry{
			Content: string(line),
			RawLine: raw,
		}, nil
	}
	timestampEnd += timestampStart

	timestampBytes := line[timestampStart:timestampEnd]
	timestampMs, err := strconv.ParseInt(string(timestampBytes), 10, 64)
	if err != nil {
		content := contentAfterBEL(line, timestampEnd)
		return &Entry{
			Content: string(content),
			RawLine: raw,
		}, nil
	}

	content := contentAfterBEL(line, timestampEnd)

	return &Entry{
		Timestamp: time.Unix(0, timestampMs*int64(time.Millisecond)),
		Content:   string(content),
		RawLine:   raw,
	}, nil
}

func hasOSCStart(data []byte) bool {
	return len(data) >= len(oscStart) && bytes.Equal(data[:len(oscStart)], oscStart)
}

func contentAfterBEL(line []byte, timestampEnd int) []byte {
	contentStart := timestampEnd + 1
	if contentStart >= len(line) {
		return nil
	}

	content := line[contentStart:]
	if nextOSC := bytes.Index(content, oscStart); nextOSC != -1 {
		content = content[:nextOSC]
	}
	return content
}
