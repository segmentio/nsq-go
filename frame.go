package nsq

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

type FrameType int

const (
	FrameTypeResponse FrameType = 0
	FrameTypeError    FrameType = 1
	FrameTypeMessage  FrameType = 2
)

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

type Frame interface {
	FrameType() FrameType

	Write(*bufio.Writer) error
}

type UnknownFrame struct {
	Type FrameType
	Data []byte
}

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

func (f UnknownFrame) FrameType() FrameType {
	return f.Type
}

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
