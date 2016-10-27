package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

// Nop represents the NOP command.
type Nop struct {
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Nop) Name() string {
	return "NOP"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Nop) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("NOP\n"); err != nil {
		err = errors.Wrap(err, "writing NOP command")
		return
	}
	return
}
