package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

type Nop struct {
}

func (c Nop) Name() string {
	return "NOP"
}

func (c Nop) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("NOP\n"); err != nil {
		err = errors.Wrap(err, "writing NOP command")
		return
	}
	return
}
