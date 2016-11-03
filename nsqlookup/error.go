package nsqlookup

import (
	"bufio"
	"bytes"
	"fmt"
)

type Error struct {
	Code   string
	Reason string
}

const (
	ErrInvalid    = "E_INVALID"
	ErrBadTopic   = "E_BAD_TOPIC"
	ErrBadChannel = "E_BAD_CHANNEL"
	ErrBadBody    = "E_BAD_BODY"
)

func (e Error) Error() string {
	return e.Code + " " + e.Reason
}

func (e Error) Status() string {
	return e.Code
}

func (e Error) Write(w *bufio.Writer) (err error) {
	return writeResponse(w, []byte(e.Error()))
}

func makeErrInvalid(s string, a ...interface{}) Error {
	return makeError(ErrInvalid, s, a...)
}

func makeErrBadTopic(s string, a ...interface{}) Error {
	return makeError(ErrBadTopic, s, a...)
}

func makeErrBadChannel(s string, a ...interface{}) Error {
	return makeError(ErrBadChannel, s, a...)
}

func makeErrBadBody(s string, a ...interface{}) Error {
	return makeError(ErrBadBody, s, a...)
}

func makeError(c string, s string, a ...interface{}) Error {
	return Error{
		Code:   c,
		Reason: fmt.Sprintf(s, a...),
	}
}

func readError(data []byte) Error {
	off := bytes.IndexByte(data, ' ')
	if off < 0 {
		off = len(data)
	}
	code, reason := data[:off], data[off+1:]
	return Error{
		Code:   string(code),
		Reason: string(reason),
	}
}
