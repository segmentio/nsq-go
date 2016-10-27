package nsq

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// MessageID is used to represent NSQ message IDs.
type MessageID uint64

// ParseMessageID attempts to parse s, which should be an hexadecimal
// representation of an 8 byte message ID.
func ParseMessageID(s string) (id MessageID, err error) {
	var v uint64
	v, err = strconv.ParseUint(s, 16, 64)
	id = MessageID(v)
	return
}

// String returns the hexadecimal representation of the message ID as a string.
func (id MessageID) String() string {
	return strconv.FormatUint(uint64(id), 16)
}

// WriteTo writes the message ID to w.
//
// This method satisfies the io.WriterTo interface.
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

// Message is a frame type representing a NSQ message.
type Message struct {
	// The ID of the message.
	ID MessageID

	// Attempts is set to the number of attempts made to deliver the message.
	Attempts uint16

	// Body contains the raw data of the message frame.
	Body []byte

	// Timestamp is the time at which the message was published.
	Timestamp time.Time

	// Unexported fields set by the consumer connections.
	cmdChan chan<- Command
}

// Finish must be called on every message received from a consumer to let the
// NSQ server know that the message was successfully processed.
//
// One of Finish or Request should be called on every message, and the methods
// will panic if they are called more than once.
func (m *Message) Finish() {
	if m.cmdChan == nil {
		panic("(*Message).Finish or (*Message).Requeue has already been called")
	}
	defer func() { recover() }() // the connection may have been closed asynchronously
	m.cmdChan <- Fin{MessageID: m.ID}
	m.cmdChan = nil
}

// Requeue must be called on messages received from a consumer to let the NSQ
// server know that the message could not be proessed and should be retried.
// The timeout is the amount of time the NSQ server waits before offering this
// message again to its consumers.
//
// One of Finish or Request should be called on every message, and the methods
// will panic if they are called more than once.
func (m *Message) Requeue(timeout time.Duration) {
	if m.cmdChan == nil {
		panic("(*Message).Finish or (*Message).Requeue has already been called")
	}
	defer func() { recover() }() // the connection may have been closed asynchronously
	m.cmdChan <- Req{MessageID: m.ID, Timeout: timeout}
	m.cmdChan = nil
}

// FrameType returns FrameTypeMessage, satisfies the Frame interface.
func (m Message) FrameType() FrameType {
	return FrameTypeMessage
}

// Write serializes the frame to the given buffered output, satisfies the Frame
// interface.
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
