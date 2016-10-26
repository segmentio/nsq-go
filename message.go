package nsq

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type MessageID uint64

func ParseMessageID(hex string) (id MessageID, err error) {
	var v uint64
	v, err = strconv.ParseUint(hex, 16, 64)
	id = MessageID(v)
	return
}

func (id MessageID) String() string {
	return strconv.FormatUint(uint64(id), 16)
}

func (id MessageID) WriteTo(w io.Writer) (int64, error) {
	a := [16]byte{}
	b := strconv.AppendUint(a[:0], uint64(id), 16)
	n := len(a) - len(b)
	copy(a[n:], b)

	for i := 0; i != n; i++ {
		a[i] = '0'
	}

	c, e := w.Write(a[:])
	return int64(c), e
}

type Message struct {
	ID        MessageID
	Attempts  uint16
	Body      []byte
	Timestamp time.Time
}

func (m Message) Write(w *bufio.Writer) (err error) {
	if err = writeFrameHeader(w, FrameTypeMessage, len(m.Body)+26); err != nil {
		err = errors.WithMessage(err, "writing message")
		return
	}

	if err = binary.Write(w, binary.BigEndian, m.Timestamp.UnixNano()); err != nil {
		err = errors.Wrap(err, "writing message")
		return
	}

	if err = binary.Write(w, binary.BigEndian, m.Attempts); err != nil {
		err = errors.Wrap(err, "writing message")
		return
	}

	if _, err = m.ID.WriteTo(w); err != nil {
		err = errors.Wrap(err, "writing message")
		return
	}

	if _, err = w.Write(m.Body); err != nil {
		err = errors.Wrap(err, "writing message")
		return
	}

	return
}

func readMessage(n int, r *bufio.Reader) (msg Message, err error) {
	var timestamp int64
	var attempts uint16
	var msgID MessageID
	var hexID [16]byte
	var body = make([]byte, n-26)

	if err = binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		err = errors.Wrap(err, "reading message timestamp")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &attempts); err != nil {
		err = errors.Wrap(err, "reading message attempts")
		return
	}

	if _, err = io.ReadFull(r, hexID[:]); err != nil {
		err = errors.Wrap(err, "reading message ID")
		return
	}

	if _, err = io.ReadFull(r, body); err != nil {
		err = errors.Wrap(err, "reading message body")
		return
	}

	if msgID, err = ParseMessageID(string(hexID[:])); err != nil {
		err = errors.Wrap(err, "parsing message ID")
		return
	}

	msg = Message{
		ID:        msgID,
		Attempts:  attempts,
		Body:      body,
		Timestamp: time.Unix(0, timestamp),
	}
	return
}
