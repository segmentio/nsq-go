package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

type Touch struct {
	MessageID MessageID
}

func (c Touch) Name() string {
	return "TOUCH"
}

func (c Touch) write(w *bufio.Writer) (err error) {
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
