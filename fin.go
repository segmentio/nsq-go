package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

// Fin represents the FIN command.
type Fin struct {
	// MessageID is the ID of the message to mark finished.
	MessageID MessageID
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Fin) Name() string {
	return "FIN"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Fin) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("FIN "); err != nil {
		err = errors.Wrap(err, "writing FIN command")
		return
	}

	if _, err = c.MessageID.WriteTo(w); err != nil {
		err = errors.Wrap(err, "writing FIN message ID")
		return
	}

	if err = w.WriteByte('\n'); err != nil {
		err = errors.Wrap(err, "writing FIN command")
		return
	}

	return
}

func readFin(line string) (cmd Fin, err error) {
	if cmd.MessageID, err = ParseMessageID(line); err != nil {
		err = errors.Wrap(err, "reading FIN message ID")
		return
	}
	return
}
