package nsq

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Pub struct {
	Topic   string
	Message []byte
}

func (c Pub) Name() string {
	return "PUB"
}

func (c Pub) Write(w *bufio.Writer) (err error) {
	for _, s := range [...]string{
		"PUB ",
		c.Topic,
		"\n",
	} {
		if _, err = w.WriteString(s); err != nil {
			err = errors.Wrap(err, "writing PUB command")
			return
		}
	}

	if err = binary.Write(w, binary.BigEndian, uint32(len(c.Message))); err != nil {
		err = errors.Wrap(err, "writing PUB message size")
		return
	}

	if _, err = w.Write(c.Message); err != nil {
		err = errors.Wrap(err, "writing PUB message data")
		return
	}

	return
}

func readPub(line string, r *bufio.Reader) (cmd Pub, err error) {
	var topic string
	var size uint32
	var data []byte

	topic, line = readNextWord(line)

	if len(topic) == 0 {
		err = errors.New("missing topic in PUB command")
		return
	}

	if len(line) != 0 {
		err = errors.New("too many arguments found in PUB command")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = errors.Wrap(err, "reading PUB message size")
		return
	}

	data = make([]byte, int(size))

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading PUB message data")
		return
	}

	cmd = Pub{
		Topic:   topic,
		Message: data,
	}
	return
}
