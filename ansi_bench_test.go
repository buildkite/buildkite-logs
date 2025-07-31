package buildkitelogs

import (
	"strings"
	"testing"
)

var (
	// Benchmark inputs of different types and sizes
	benchInputs = map[string]string{
		"no_ansi":       strings.Repeat("hello world this is a log line without any ansi codes\n", 100),
		"simple_ansi":   strings.Repeat("\x1b[31mred text\x1b[0m normal text\n", 100),
		"complex_ansi":  strings.Repeat("\x1b[1;31;40mbold red on black\x1b[0m \x1b[32;1mgreen bold\x1b[0m \x1b[90mdim\x1b[0m\n", 100),
		"buildkite_log": strings.Repeat("\x1b[90m2023-01-01 12:00:00.123\x1b[0m \x1b[32m[INFO]\x1b[0m \x1b[1mStarting build step\x1b[0m\n", 100),
		"error_log":     strings.Repeat("\x1b[31;1mError:\x1b[0m \x1b[31mtest failed\x1b[0m\nDetails: \x1b[33mcheck logs\x1b[0m\n", 100),
		"mixed_content": strings.Repeat("normal text \x1b[31mred\x1b[0m more normal \x1b[32mgreen\x1b[0m end\n", 100),
		"heavy_ansi":    strings.Repeat("\x1b[31m\x1b[32m\x1b[33m\x1b[34m\x1b[35m\x1b[36m\x1b[37m\x1b[0mtext\x1b[2J\x1b[H\x1b]0;title\x07\n", 100),
		"single_line":   "hello world",
		"single_ansi":   "\x1b[31mred\x1b[0m",
		"large_line":    strings.Repeat("x", 10000) + "\x1b[31mred\x1b[0m" + strings.Repeat("y", 10000),
	}
)

// Benchmark StripANSI (custom parser) with various input types
func BenchmarkStripANSI(b *testing.B) {
	for name, input := range benchInputs {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = StripANSI(input)
			}
		})
	}
}

// Benchmark StripANSIRegex with various input types
func BenchmarkStripANSIRegex(b *testing.B) {
	for name, input := range benchInputs {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = StripANSIRegex(input)
			}
		})
	}
}

// Benchmark memory allocations comparison
func BenchmarkStripANSIAllocs(b *testing.B) {
	testInput := benchInputs["complex_ansi"]

	b.Run("Custom", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(testInput)))
		for i := 0; i < b.N; i++ {
			_ = StripANSI(testInput)
		}
	})

	b.Run("Regex", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(testInput)))
		for i := 0; i < b.N; i++ {
			_ = StripANSIRegex(testInput)
		}
	})
}

// Benchmark with different ANSI code densities
func BenchmarkStripANSIDensity(b *testing.B) {
	// Generate inputs with different ANSI code densities
	densities := map[string]string{
		"0_percent":   strings.Repeat("normal text line\n", 1000),
		"10_percent":  generateDensityInput(1000, 0.1),
		"25_percent":  generateDensityInput(1000, 0.25),
		"50_percent":  generateDensityInput(1000, 0.5),
		"75_percent":  generateDensityInput(1000, 0.75),
		"100_percent": strings.Repeat("\x1b[31m\x1b[0m", 1000),
	}

	for density, input := range densities {
		b.Run(density, func(b *testing.B) {
			b.SetBytes(int64(len(input)))
			for i := 0; i < b.N; i++ {
				_ = StripANSI(input)
			}
		})
	}
}

// Helper function to generate input with specified ANSI density
func generateDensityInput(lines int, ansiDensity float64) string {
	var builder strings.Builder
	for i := 0; i < lines; i++ {
		if float64(i%100)/100.0 < ansiDensity {
			builder.WriteString("\x1b[31mline with ansi\x1b[0m\n")
		} else {
			builder.WriteString("normal line\n")
		}
	}
	return builder.String()
}

// Benchmark realistic log processing scenario
func BenchmarkLogProcessingScenario(b *testing.B) {
	// Simulate processing 1000 log lines with typical buildkite formatting
	logLines := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		switch i % 4 {
		case 0:
			logLines[i] = "\x1b[90m2023-01-01 12:00:00.123\x1b[0m \x1b[32m[INFO]\x1b[0m Normal log line"
		case 1:
			logLines[i] = "\x1b[90m2023-01-01 12:00:01.456\x1b[0m \x1b[33m[WARN]\x1b[0m Warning message"
		case 2:
			logLines[i] = "\x1b[90m2023-01-01 12:00:02.789\x1b[0m \x1b[31m[ERROR]\x1b[0m Error occurred"
		case 3:
			logLines[i] = "Plain log line without ANSI codes"
		}
	}

	totalBytes := 0
	for _, line := range logLines {
		totalBytes += len(line)
	}
	b.SetBytes(int64(totalBytes))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range logLines {
			_ = StripANSI(line)
		}
	}
}

// Benchmark CleanContent and CleanGroup methods
func BenchmarkParquetLogEntryClean(b *testing.B) {
	entry := ParquetLogEntry{
		Content: "\x1b[90m2023-01-01 12:00:00\x1b[0m \x1b[32m[INFO]\x1b[0m Starting build step  ",
		Group:   "  \x1b[1mTest Group\x1b[0m  ",
	}

	b.Run("CleanContent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = entry.CleanContent(true)
		}
	})

	b.Run("CleanGroup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = entry.CleanGroup(true)
		}
	})

	b.Run("Both", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = entry.CleanContent(true)
			_ = entry.CleanGroup(true)
		}
	})
}
