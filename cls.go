package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

// Cls represents the CLS command.
type Cls struct {
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Cls) Name() string {
	return "CLS"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Cls) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("CLS\n"); err != nil {
		err = errors.Wrap(err, "writing CLS command")
		return
	}
	return
}
