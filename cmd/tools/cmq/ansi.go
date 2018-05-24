package main

import (
	"fmt"
)

// SaveScreen saves the screen
func SaveScreen() {
	print(`\x1b[?47h`)
}

// RestoreScreen restores saved screen
func RestoreScreen() {
	print(`\x1b[?47l`)
}

// ClearScreen clears screen
func ClearScreen() {
	print("\x1b[H;2J\x1b[2J") // clear screen and move cursor to (0,0)
}

// MoveCursor moves cursor to specified row/col
func MoveCursor(row, col int) {
	fmt.Printf("\x1b[%d;%dH", row, col)
}

const (
	// Black color
	Black = iota
	// Red color
	Red
	// Green color
	Green
	// Yellow color
	Yellow
	// Blue color
	Blue
	// Magenta color
	Magenta
	// Cyan color
	Cyan
	// White color
	White
	// BrightBlack color
	BrightBlack
	// BrightRed color
	BrightRed
	// BrightGreen color
	BrightGreen
	// BrightYellow color
	BrightYellow
	// BrightBlue color
	BrightBlue
	// BrightMagenta color
	BrightMagenta
	// BrightCyan color
	BrightCyan
	// BrightWhite color
	BrightWhite
)

// AnsiCtrlSeq holds an ansi control sequence
type AnsiCtrlSeq struct {
	text string
	ansi []int
}

// Text takes a string to use with ansi control sequence
func Text(text string) *AnsiCtrlSeq {
	return &AnsiCtrlSeq{text: text}
}

// String returns the string to output with the appropriate ansi control sequences
func (t *AnsiCtrlSeq) String() string {

	const (
		start = "\x1b["
		end   = "m"
		reset = start + end
	)

	switch len(t.ansi) {
	case 0:
		return t.text

	case 1:
		return start + fmt.Sprintf("%d", t.ansi[0]) + end + t.text + reset

	default:

		var str = start

		for i, c := range t.ansi {

			if i > 0 {
				str += ";"
			}

			str += fmt.Sprintf("%d", c)
		}

		return str + end + t.text + reset
	}
}

// Width truncates to specified width
func (t *AnsiCtrlSeq) Width(w int) *AnsiCtrlSeq {
	t.text = fmt.Sprintf("%*s", w, t.text)
	return t
}

// Color adds specified foreground color to text
func (t *AnsiCtrlSeq) Color(color int) *AnsiCtrlSeq {

	if color <= White {
		t.ansi = append(t.ansi, 30+color)
		return t
	}

	return t.Color(30 + color - BrightBlack).Bold()
}

// Background adds specified background color to text
func (t *AnsiCtrlSeq) Background(color int) *AnsiCtrlSeq {

	if color <= White {
		t.ansi = append(t.ansi, 40+color)
		return t
	}

	// can't do "bright" background; so just use plain
	return t.Background(40 + color - White)
}

// Underline underlines text
func (t *AnsiCtrlSeq) Underline() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 4)
	return t
}

// Bold emboldens the text
func (t *AnsiCtrlSeq) Bold() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 1)
	return t
}

// Blink makes text blink
func (t *AnsiCtrlSeq) Blink() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 5)
	return t
}
