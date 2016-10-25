package nsq

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Auth struct {
	Secret string
}

func (c Auth) Name() string {
	return "AUTH"
}

func (c Auth) write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("AUTH\n"); err != nil {
		err = errors.Wrap(err, "writing AUTH command")
		return
	}

	if err = binary.Write(w, binary.BigEndian, uint32(len(c.Secret))); err != nil {
		err = errors.Wrap(err, "writing AUTH secret size")
		return
	}

	if _, err = w.WriteString(c.Secret); err != nil {
		err = errors.Wrap(err, "writing AUTH secret data")
		return
	}

	return
}

func readAuth(r *bufio.Reader) (cmd Auth, err error) {
	var size uint32
	var data []byte

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = errors.Wrap(err, "reading AUTH secret size")
		return
	}

	data = make([]byte, int(size))

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading AUTH secret data")
		return
	}

	cmd = Auth{
		Secret: string(data),
	}
	return
}
