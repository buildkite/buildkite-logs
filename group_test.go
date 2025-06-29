package buildkitelogs

import (
	"strings"
	"testing"
)

func TestGroupTracking(t *testing.T) {
	parser := NewParser()

	testData := []string{
		"\x1b_bk;t=1745322209921\x07~~~ Running global environment hook",
		"\x1b_bk;t=1745322209922\x07[90m$[0m /buildkite/agent/hooks/environment",
		"\x1b_bk;t=1745322209923\x07Some regular output",
		"\x1b_bk;t=1745322209924\x07--- :package: Build job checkout directory",
		"\x1b_bk;t=1745322209925\x07Another line of output",
		"\x1b_bk;t=1745322209926\x07+++ :hammer: Example tests",
		"\x1b_bk;t=1745322209927\x07Test output line",
	}

	expectedGroups := []string{
		"~~~ Running global environment hook",      // Section header itself
		"~~~ Running global environment hook",      // Should inherit from previous section
		"~~~ Running global environment hook",      // Should inherit from previous section
		"--- :package: Build job checkout directory", // New section header
		"--- :package: Build job checkout directory", // Should inherit from new section
		"+++ :hammer: Example tests",                  // Another new section header
		"+++ :hammer: Example tests",                  // Should inherit from latest section
	}

	for i, line := range testData {
		entry, err := parser.ParseLine(line)
		if err != nil {
			t.Fatalf("ParseLine() error = %v", err)
		}

		if entry.Group != expectedGroups[i] {
			t.Errorf("Line %d: expected group %q, got %q", i+1, expectedGroups[i], entry.Group)
		}
	}
}

func TestGroupTrackingWithIterator(t *testing.T) {
	parser := NewParser()
	
	testData := `~~~ Running global environment hook
[90m$[0m /buildkite/agent/hooks/environment
Some regular output
--- :package: Build job checkout directory
Another line of output
+++ :hammer: Example tests
Test output line`

	reader := strings.NewReader(testData)
	iterator := parser.NewIterator(reader)

	expectedGroups := []string{
		"~~~ Running global environment hook",
		"~~~ Running global environment hook",
		"~~~ Running global environment hook",
		"--- :package: Build job checkout directory",
		"--- :package: Build job checkout directory",
		"+++ :hammer: Example tests",
		"+++ :hammer: Example tests",
	}

	i := 0
	for iterator.Next() {
		entry := iterator.Entry()
		
		if i >= len(expectedGroups) {
			t.Fatalf("More entries than expected")
		}

		if entry.Group != expectedGroups[i] {
			t.Errorf("Entry %d: expected group %q, got %q", i+1, expectedGroups[i], entry.Group)
		}
		i++
	}

	if err := iterator.Err(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	if i != len(expectedGroups) {
		t.Fatalf("Expected %d entries, got %d", len(expectedGroups), i)
	}
}