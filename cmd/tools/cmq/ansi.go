package main

import (
	"fmt"
)

func SaveScreen() {
	print(`\x1b[?47h`)
}

func RestoreScreen() {
	print(`\x1b[?47l`)
}
func ClearScreen() {
	print("\x1b[H;2J\x1b[2J") // clear screen and move cursor to (0,0)
}

func MoveCursor(row, col int) {
	fmt.Printf("\x1b[%d;%dH", row, col)
}

const (
	Black = iota
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	White
	BrightBlack
	BrightRed
	BrightGreen
	BrightYellow
	BrightBlue
	BrightMagenta
	BrightCyan
	BrightWhite
)

type AnsiCtrlSeq struct {
	text string
	ansi []int
}

func Text(text string) *AnsiCtrlSeq {
	return &AnsiCtrlSeq{text: text}
}

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

func (t *AnsiCtrlSeq) Width(w int) *AnsiCtrlSeq {
	t.text = fmt.Sprintf("%*s", w, t.text)
	return t
}

func (t *AnsiCtrlSeq) Color(color int) *AnsiCtrlSeq {

	if color <= White {
		t.ansi = append(t.ansi, 30+color)
		return t
	}

	return t.Color(30 + color - BrightBlack).Bold()
}

func (t *AnsiCtrlSeq) Background(color int) *AnsiCtrlSeq {

	if color <= White {
		t.ansi = append(t.ansi, 40+color)
		return t
	}

	// can't do "bright" background; so just use plain
	return t.Background(40 + color - White)
}

func (t *AnsiCtrlSeq) Underline() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 4)
	return t
}

func (t *AnsiCtrlSeq) Bold() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 1)
	return t
}

func (t *AnsiCtrlSeq) Blink() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 5)
	return t
}

func (t *AnsiCtrlSeq) FastBlink() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 6)
	return t
}

func (t *AnsiCtrlSeq) Reverse() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 7)
	return t
}

func (t *AnsiCtrlSeq) AltFont() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 12)
	return t
}

func (t *AnsiCtrlSeq) Overlined() *AnsiCtrlSeq {

	t.ansi = append(t.ansi, 53)
	return t
}
