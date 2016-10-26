package nsq

import (
	"bufio"
	"strconv"

	"github.com/pkg/errors"
)

type Rdy struct {
	Count int
}

func (c Rdy) Name() string {
	return "RDY"
}

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
