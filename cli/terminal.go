package cli

import (
	"fmt"
	"io"

	"golang.org/x/term"
)

// isTerminal reports whether fd is connected to an interactive terminal.
// Uses golang.org/x/term which is portable on macOS and Linux without build tags.
func isTerminal(fd int) bool {
	return term.IsTerminal(fd)
}

// clearTerminal writes ANSI escape codes to move the cursor to position (1,1)
// and clear the entire screen. Works on any VT100-capable terminal (macOS, Linux).
// Does not use os/exec or the system "clear" binary.
func clearTerminal(w io.Writer) {
	fmt.Fprint(w, "\033[H\033[2J")
}
