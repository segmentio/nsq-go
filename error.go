package nsq

import (
	"bufio"
	"io"
	"strings"

	"github.com/pkg/errors"
)

// Error is a frame type representing error responses to commands.
type Error string

const (
	ErrInvalid      Error = "E_INVALID"
	ErrBadBody      Error = "E_BAD_BODY"
	ErrBadTopic     Error = "E_BAD_TOPIC"
	ErrBadChannel   Error = "E_BAD_CHANNEL"
	ErrBadMessage   Error = "E_BAD_MESSAGE"
	ErrPubFailed    Error = "E_PUB_FAILED"
	ErrMPubFailed   Error = "E_MPUB_FAILED"
	ErrFinFailed    Error = "E_FIN_FAILED"
	ErrReqFailed    Error = "E_REQ_FAILED"
	ErrTouchFailed  Error = "E_TOUCH_FAILED"
	ErrAuthFailed   Error = "E_AUTH_FAILED"
	ErrUnauthorized Error = "E_UNAUTHORIZED"
)

// String returns the error as a string, satisfies the error interface.
func (e Error) Error() string {
	return string(e)
}

// String returns the error as a string.
func (e Error) String() string {
	return string(e)
}

// FrameType returns FrameTypeError, satisfies the Frame interface.
func (e Error) FrameType() FrameType {
	return FrameTypeError
}

// Write serializes the frame to the given buffered output, satisfies the Frame
// interface.
func (e Error) Write(w *bufio.Writer) (err error) {
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

type errorList []error

func (e errorList) Error() string {
	list := make([]string, 0, len(e))

	for _, err := range e {
		list = append(list, err.Error())
	}

	return strings.Join(list, "; ")
}

func appendError(errList error, err error) error {
	if err == nil {
		return errList
	}
	switch e := errList.(type) {
	case errorList:
		return append(e, err)
	case error:
		return errorList{e, err}
	default:
		return errorList{err}
	}
}

func isTimeout(err error) bool {
	e, ok := err.(interface {
		Timeout() bool
	})
	return ok && e.Timeout()
}
