package nsq

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

// FrameType is used to represent the different types of frames that a consumer
// may receive.
type FrameType int

const (
	// FrameTypeResponse is the code for frames that carry success responses to
	// commands.
	FrameTypeResponse FrameType = 0

	// FrameTypeError is the code for frames that carry error responses to
	// commands.
	FrameTypeError FrameType = 1

	// FrameTypeMessage is the code for frames that carry messages.
	FrameTypeMessage FrameType = 2
)

// String returns a human-readable representation of the frame type.
func (t FrameType) String() string {
	switch t {
	case FrameTypeResponse:
		return "response"

	case FrameTypeError:
		return "error"

	case FrameTypeMessage:
		return "message"

	default:
		return "frame <" + strconv.Itoa(int(t)) + ">"
	}
}

// The Frame interface is implemented by types that represent the different
// types of frames that a consumer may receive.
type Frame interface {
	// FrameType returns the code representing the frame type.
	FrameType() FrameType

	// Write serializes the frame to the given buffered output.
	Write(*bufio.Writer) error
}

// ReadFrame reads a frame from the buffer input r, returning it or an error if
// something went wrong.
//
//	if frame, err := ReadFrame(r); err != nil {
//		// handle the error
//		...
//	} else {
//		switch f := frame.(type) {
//		case nsq.Message:
//			...
//		}
//	}
//
func ReadFrame(r *bufio.Reader) (frame Frame, err error) {
	var size int32
	var ftype int32

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = errors.Wrap(err, "reading frame size")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &ftype); err != nil {
		err = errors.Wrap(err, "reading frame type")
		return
	}

	switch FrameType(ftype) {
	case FrameTypeResponse:
		return readResponse(int(size), r)

	case FrameTypeError:
		return readError(int(size), r)

	case FrameTypeMessage:
		return readMessage(int(size), r)

	default:
		return readUnknownFrame(FrameType(ftype), int(size), r)
	}
}

// UnknownFrame is used to represent frames of unknown types for which the
// library has no special implementation.
type UnknownFrame struct {
	// Type is the type of the frame.
	Type FrameType

	// Data contains the raw data of the frame.
	Data []byte
}

// FrameType returns the code representing the frame type, satisfies the Frame
// interface.
func (f UnknownFrame) FrameType() FrameType {
	return f.Type
}

// Write serializes the frame to the given buffered output, satisfies the Frame
// interface.
func (f UnknownFrame) Write(w *bufio.Writer) (err error) {
	if err = writeFrameHeader(w, f.Type, len(f.Data)); err != nil {
		err = errors.WithMessage(err, "writing unknown frame")
		return
	}

	if _, err = w.Write(f.Data); err != nil {
		err = errors.Wrap(err, "writing unknown frame")
		return
	}

	return
}

func readUnknownFrame(t FrameType, n int, r *bufio.Reader) (f UnknownFrame, err error) {
	b := make([]byte, n)

	if _, err = io.ReadFull(r, b); err != nil {
		err = errors.Wrap(err, "reading unknown frame")
		return
	}

	f = UnknownFrame{
		Type: t,
		Data: b,
	}
	return
}

func writeFrameHeader(w *bufio.Writer, ftype FrameType, size int) (err error) {
	if err = binary.Write(w, binary.BigEndian, int32(size)); err != nil {
		err = errors.Wrap(err, "writing frame header")
		return
	}

	if err = binary.Write(w, binary.BigEndian, int32(ftype)); err != nil {
		err = errors.Wrap(err, "writing frame header")
		return
	}

	return
}
