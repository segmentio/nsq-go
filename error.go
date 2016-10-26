package nsq

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

type Error string

const (
	ErrInvalid      Error = Error("E_INVALID")
	ErrBadBody      Error = Error("E_BAD_BODY")
	ErrBadTopic     Error = Error("E_BAD_TOPIC")
	ErrBadChannel   Error = Error("E_BAD_CHANNEL")
	ErrBadMessage   Error = Error("E_BAD_MESSAGE")
	ErrPubFailed    Error = Error("E_PUB_FAILED")
	ErrMPubFailed   Error = Error("E_MPUB_FAILED")
	ErrFinFailed    Error = Error("E_FIN_FAILED")
	ErrReqFailed    Error = Error("E_REQ_FAILED")
	ErrTouchFailed  Error = Error("E_TOUCH_FAILED")
	ErrAuthFailed   Error = Error("E_AUTH_FAILED")
	ErrUnauthorized Error = Error("E_UNAUTHORIZED")
)

func (e Error) Error() string {
	return string(e)
}

func (e Error) String() string {
	return string(e)
}

func (e Error) write(w *bufio.Writer) (err error) {
	if err = writeFrameHeader(w, FrameTypeError, len(e)); err != nil {
		err = errors.WithMessage(err, "writing error message")
		return
	}

	if _, err = w.WriteString(string(e)); err != nil {
		err = errors.Wrap(err, "writing error message")
		return
	}

	return
}

func readError(n int, r *bufio.Reader) (e Error, err error) {
	data := make([]byte, n)

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading error message")
		return
	}

	e = Error(data)
	return
}
