package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

const (
	ExitSuccess     = 0
	ExitKeyNotFound = 1
	ExitUnreachable = 2
	ExitBadArgs     = 3
	ExitServerError = 4
	ExitCancelled   = 5
)

// CLIError is the error type returned by all RunE functions.
// HandleErr reads Code to determine which os.Exit value to use.
type CLIError struct {
	Msg  string
	Code int
}

func (e *CLIError) Error() string { return e.Msg }

// HandleErr prints to stderr and exits. Called from cmd/cli/main.go after Execute().
func HandleErr(err error) {
	if err == nil {
		return
	}
	var cliErr *CLIError
	if errors.As(err, &cliErr) {
		fmt.Fprintln(os.Stderr, cliErr.Msg)
		os.Exit(cliErr.Code)
	}
	fmt.Fprintln(os.Stderr, "error:", err.Error())
	os.Exit(ExitServerError)
}

// translateErr converts a client sentinel error into a *CLIError with context.
// host is used in the unreachable message; key is used in the not-found message
// (pass "" for key when the command does not operate on a specific key).
func translateErr(host, key string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, client.ErrNotFound):
		msg := "key not found"
		if key != "" {
			msg = fmt.Sprintf("key %q not found", key)
		}
		return &CLIError{Msg: msg, Code: ExitKeyNotFound}
	case errors.Is(err, client.ErrUnreachable):
		return &CLIError{
			Msg:  fmt.Sprintf("cannot reach node at %s", host),
			Code: ExitUnreachable,
		}
	case errors.Is(err, client.ErrServerError):
		return &CLIError{Msg: err.Error(), Code: ExitServerError}
	default:
		return &CLIError{Msg: err.Error(), Code: ExitServerError}
	}
}

func badArgs(format string, args ...any) *CLIError {
	return &CLIError{Msg: fmt.Sprintf(format, args...), Code: ExitBadArgs}
}

func cancelled(msg string) *CLIError {
	return &CLIError{Msg: msg, Code: ExitCancelled}
}
