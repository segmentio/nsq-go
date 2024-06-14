package nsq

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// DPub represents the DPUB command.
type DPub struct {
	// Topic must be set to the name of the topic to which the message will be
	// published.
	Topic string

	// Delay is the duration NSQ will defer the message before sending it to a
	// client.
	Delay time.Duration

	// Message is the raw message to publish.
	Message []byte
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c DPub) Name() string {
	return "DPUB"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c DPub) Write(w *bufio.Writer) (err error) {
	for _, s := range [...]string{
		"DPUB ",
		c.Topic,
		" ",
		strconv.FormatUint(uint64(c.Delay/time.Millisecond), 10),
		"\n",
	} {
		if _, err = w.WriteString(s); err != nil {
			err = errors.Wrap(err, "writing DPUB command")
			return
		}
	}

	if err = binary.Write(w, binary.BigEndian, uint32(len(c.Message))); err != nil {
		err = errors.Wrap(err, "writing DPUB message size")
		return
	}

	if _, err = w.Write(c.Message); err != nil {
		err = errors.Wrap(err, "writing DPUB message data")
		return
	}

	return
}

func readDPub(line string, r *bufio.Reader) (cmd DPub, err error) {
	var topic string
	var delayStr string
	var delayMsecs uint64
	var size uint32
	var data []byte

	topic, line = readNextWord(line)
	delayStr, line = readNextWord(line)

	if len(topic) == 0 {
		err = errors.New("missing topic in DPUB command")
		return
	}

	if len(delayStr) == 0 {
		err = errors.New("missing delay in DPUB command")
		return
	}

	if len(line) != 0 {
		err = errors.New("too many arguments found in DPUB command")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = errors.Wrap(err, "reading DPUB message size")
		return
	}

	data = make([]byte, int(size))

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading DPUB message data")
		return
	}

	if delayMsecs, err = strconv.ParseUint(delayStr, 10, 64); err != nil {
		err = errors.Wrap(err, "reading DPUB delay")
		return
	}

	cmd = DPub{
		Topic:   topic,
		Delay:   time.Duration(delayMsecs) * time.Millisecond,
		Message: data,
	}
	return
}
