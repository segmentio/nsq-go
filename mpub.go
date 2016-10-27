package nsq

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

// MPub represents the MPUB command.
type MPub struct {
	// Topic must be set to the name of the topic to which the messages will be
	// published.
	Topic string

	// Messages is the list of raw messages to publish.
	Messages [][]byte
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c MPub) Name() string {
	return "MPUB"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c MPub) Write(w *bufio.Writer) (err error) {
	for _, s := range [...]string{
		"MPUB ",
		c.Topic,
		"\n",
	} {
		if _, err = w.WriteString(s); err != nil {
			err = errors.Wrap(err, "writing MPUB command")
			return
		}
	}

	var size uint32

	for _, m := range c.Messages {
		size += uint32(len(m))
	}

	if err = binary.Write(w, binary.BigEndian, size); err != nil {
		err = errors.Wrap(err, "writing MPUB body size")
		return
	}

	if err = binary.Write(w, binary.BigEndian, uint32(len(c.Messages))); err != nil {
		err = errors.Wrap(err, "writing MPUB message count")
		return
	}

	for _, m := range c.Messages {
		if err = binary.Write(w, binary.BigEndian, uint32(len(m))); err != nil {
			err = errors.Wrap(err, "writing MPUB message size")
			return
		}

		if _, err = w.Write(m); err != nil {
			err = errors.Wrap(err, "writing MPUB message data")
			return
		}
	}

	return
}

func readMPub(line string, r *bufio.Reader) (cmd MPub, err error) {
	var topic string
	var count uint32
	var messages [][]byte

	topic, line = readNextWord(line)

	if len(topic) == 0 {
		err = errors.New("missing topic in MPUB command")
		return
	}

	if len(line) != 0 {
		err = errors.New("too many arguments found in MPUB command")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &count); err != nil {
		err = errors.Wrap(err, "reading MPUB body size")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &count); err != nil {
		err = errors.Wrap(err, "reading MPUB message count")
		return
	}

	for messages = make([][]byte, 0, int(count)); count != 0; count-- {
		var size uint32
		var data []byte

		if err = binary.Read(r, binary.BigEndian, &size); err != nil {
			err = errors.Wrap(err, "reading MPUB message size")
			return
		}

		data = make([]byte, int(size))

		if _, err = io.ReadFull(r, data); err != nil {
			err = errors.Wrap(err, "reading MPUB message data")
			return
		}

		messages = append(messages, data)
	}

	cmd = MPub{
		Topic:    topic,
		Messages: messages,
	}
	return
}
