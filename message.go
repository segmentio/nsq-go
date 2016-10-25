package nsq

import (
	"io"
	"strconv"
)

type MessageID uint64

func ParseMessageID(hex string) (id MessageID, err error) {
	var v uint64
	v, err = strconv.ParseUint(hex, 16, 64)
	id = MessageID(v)
	return
}

func (id MessageID) String() string {
	return strconv.FormatUint(uint64(id), 16)
}

func (id MessageID) WriteTo(w io.Writer) (int64, error) {
	b := strconv.AppendUint(make([]byte, 0, 64), uint64(id), 16)
	c, e := w.Write(b)
	return int64(c), e
}
