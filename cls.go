package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

type Cls struct {
}

func (c Cls) Name() string {
	return "CLS"
}

func (c Cls) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("CLS\n"); err != nil {
		err = errors.Wrap(err, "writing CLS command")
		return
	}
	return
}
