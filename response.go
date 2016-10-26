package nsq

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

type Response string

const (
	OK        = Response("OK")
	CloseWait = Response("CLOSE_WAIT")
)

func (r Response) FrameType() FrameType {
	return FrameTypeResponse
}

func (r Response) String() string {
	return string(r)
}

func (r Response) Write(w *bufio.Writer) (err error) {
	if err = writeFrameHeader(w, FrameTypeResponse, len(r)); err != nil {
		err = errors.WithMessage(err, "writing response message")
	}

	if _, err = w.WriteString(string(r)); err != nil {
		err = errors.Wrap(err, "writing response message")
		return
	}

	return
}

func readResponse(n int, r *bufio.Reader) (res Response, err error) {
	data := make([]byte, n)

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading response message")
		return
	}

	res = Response(data)
	return
}
