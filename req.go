package nsq

import (
	"bufio"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Req represents the REQ command.
type Req struct {
	// MessageID is the ID of the message to requeue.
	MessageID MessageID

	// Timeout is the duration NSQ will wait for before sending this message
	// again to a client.
	Timeout time.Duration
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Req) Name() string {
	return "REQ"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Req) Write(w *bufio.Writer) (err error) {
	if _, err = w.WriteString("REQ "); err != nil {
		err = errors.Wrap(err, "writing REQ command")
		return
	}

	if _, err = c.MessageID.WriteTo(w); err != nil {
		err = errors.Wrap(err, "writing REQ message ID")
		return
	}

	if err = w.WriteByte(' '); err != nil {
		err = errors.Wrap(err, "writing REQ command")
		return
	}

	if _, err = w.WriteString(strconv.FormatUint(uint64(c.Timeout/time.Millisecond), 10)); err != nil {
		err = errors.Wrap(err, "writing REQ timeout")
		return
	}

	if err = w.WriteByte('\n'); err != nil {
		err = errors.Wrap(err, "writing REQ command")
		return
	}

	return
}

func readReq(line string) (cmd Req, err error) {
	var s1 string
	var s2 string

	s1, line = readNextWord(line)
	s2, line = readNextWord(line)

	if len(s1) == 0 {
		err = errors.New("missing message ID in REQ command")
		return
	}

	if len(s2) == 0 {
		err = errors.New("missing timeout in REQ command")
		return
	}

	if len(line) != 0 {
		err = errors.New("too many arguments found in REQ command")
		return
	}

	var messageID MessageID
	var timeout uint64

	if messageID, err = ParseMessageID(s1); err != nil {
		err = errors.Wrap(err, "reading REQ message ID")
		return
	}

	if timeout, err = strconv.ParseUint(s2, 10, 64); err != nil {
		err = errors.Wrap(err, "reading REQ timeout")
		return
	}

	cmd = Req{
		MessageID: messageID,
		Timeout:   time.Duration(timeout) * time.Millisecond,
	}
	return
}
