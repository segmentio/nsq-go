package nsq

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

// Response is a frame type representing success responses to commands.
type Response string

const (
	// OK is returned for most successful responses.
	OK Response = "OK"

	// CloseWait is the response sent to the CLS command.
	CloseWait Response = "CLOSE_WAIT"

	// Heartbeat is the response used by NSQ servers for health checks of the
	// connections.
	Heartbeat Response = "_heartbeat_"
)

// String returns the response as a string.
func (r Response) String() string {
	return string(r)
}

// FrameType returns FrameTypeResponse, satisfies the Frame interface.
func (r Response) FrameType() FrameType {
	return FrameTypeResponse
}

// Write serializes the frame to the given buffered output, satisfies the Frame
// interface.
func (r Response) Write(w *bufio.Writer) error {
	if err := writeFrameHeader(w, FrameTypeResponse, len(r)); err != nil {
		return errors.WithMessage(err, "writing response message")
	}

	if _, err := w.WriteString(string(r)); err != nil {
		return errors.Wrap(err, "writing response message")
	}

	return nil
}

func readResponse(n int, r *bufio.Reader) (Response, error) {
	data := make([]byte, n)

	if _, err := io.ReadFull(r, data); err != nil {
		return "", errors.Wrap(err, "reading response message")
	}

	return Response(data), nil
}
