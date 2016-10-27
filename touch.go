package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

// Touch represents the TOUCH command.
type Touch struct {
	// MessageID is the ID of the message that the timeout will be reset.
	MessageID MessageID
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Touch) Name() string {
	return "TOUCH"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Touch) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("TOUCH "); err != nil {
		err = errors.Wrap(err, "writing TOUCH command")
		return
	}

	if _, err = c.MessageID.WriteTo(w); err != nil {
		err = errors.Wrap(err, "writing TOUCH message ID")
		return
	}

	if err = w.WriteByte('\n'); err != nil {
		err = errors.Wrap(err, "writing TOUCH command")
		return
	}

	return
}

func readTouch(line string) (cmd Touch, err error) {
	if cmd.MessageID, err = ParseMessageID(line); err != nil {
		err = errors.Wrap(err, "reading TOUCH message ID")
		return
	}
	return
}
