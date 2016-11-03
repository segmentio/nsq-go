package nsqlookup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

type Response interface {
	Status() string

	Write(w *bufio.Writer) error
}

type RawResponse []byte

func (RawResponse) Status() string {
	return "unknown"
}

func (r RawResponse) Write(w *bufio.Writer) error {
	return writeResponse(w, []byte(r))
}

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
