package progress

import (
	"fmt"
	"os"
	"strings"
)

// Bar represents a progress bar
type Bar struct {
	total   int
	current int
	width   int
	prefix  string
}

// New creates a new progress bar
func New(total int, prefix string) *Bar {
	return &Bar{
		total:  total,
		width:  40,
		prefix: prefix,
	}
}

// Update updates the progress bar with the current count
func (b *Bar) Update(current int) {
	b.current = current
	b.render()
}

// Increment increments the progress by 1
func (b *Bar) Increment() {
	b.current++
	b.render()
}

// Complete marks the progress bar as complete
func (b *Bar) Complete(message string) {
	b.current = b.total
	// Clear the line and print completion message
	fmt.Printf("\r%s\n", strings.Repeat(" ", b.width+len(b.prefix)+20))
	fmt.Printf("\r  ✓ %s\n", message)
}

// render draws the progress bar
func (b *Bar) render() {
	if b.total == 0 {
		return
	}

	percent := float64(b.current) / float64(b.total)
	filled := int(percent * float64(b.width))
	if filled > b.width {
		filled = b.width
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("░", b.width-filled)
	fmt.Printf("\r  %s [%s] %3.0f%% (%d/%d)", b.prefix, bar, percent*100, b.current, b.total)
	os.Stdout.Sync()
}
