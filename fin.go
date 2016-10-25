package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

type Fin struct {
	MessageID MessageID
}

func (c Fin) Name() string {
	return "FIN"
}

func (c Fin) write(w *bufio.Writer) (err error) {
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
