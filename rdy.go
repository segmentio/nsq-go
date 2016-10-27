package nsq

import (
	"bufio"
	"strconv"

	"github.com/pkg/errors"
)

// Rdy represents the RDY command.
type Rdy struct {
	// Count is the number of messages that a consumer is ready to accept.
	Count int
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Rdy) Name() string {
	return "RDY"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Rdy) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("RDY "); err != nil {
		err = errors.Wrap(err, "writing RDY command")
		return
	}

	b := strconv.AppendUint(make([]byte, 0, 64), uint64(c.Count), 10)
	b = append(b, '\n')

	if _, err = w.Write(b); err != nil {
		err = errors.Wrap(err, "writint RDY count")
		return
	}

	return
}

func readRdy(line string) (cmd Rdy, err error) {
	if cmd.Count, err = strconv.Atoi(line); err != nil {
		err = errors.Wrap(err, "reading RDY count")
		return
	}
	return
}
