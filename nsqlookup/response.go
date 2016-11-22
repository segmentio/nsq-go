package nsqlookup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

// The Response interface is implemented by all types representing nsqlookup
// server responses.
type Response interface {
	// Status returns the status of the response.
	Status() string

	// Write outputs the response to w.
	Write(w *bufio.Writer) error
}

// RawResponse is a pre-serialized byte buffer that implements the Response
// interface.
type RawResponse []byte

// Status returns the status of the response.
func (RawResponse) Status() string {
	return "unknown"
}

// Write outputs the response to w.
func (r RawResponse) Write(w *bufio.Writer) error {
	return writeResponse(w, []byte(r))
}

// ReadResponse reads res from r, or returns an error if no responses could be
// read.
func ReadResponse(r *bufio.Reader) (res Response, err error) {
	var data []byte
	var size int32

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}

	data = make([]byte, int(size))

	if _, err = io.ReadFull(r, data); err != nil {
		return
	}

	switch {
	case bytes.Equal(data, []byte("OK")):
		res = OK{}

	case bytes.HasPrefix(data, []byte("E_")):
		res = readError(data)

	default:
		res = RawResponse(data)
	}

	return
}
