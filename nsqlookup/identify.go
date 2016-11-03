package nsqlookup

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
)

type Identify struct {
	Info PeerInfo
}

func (c Identify) Name() string {
	return "IDENTIFY"
}

func (c Identify) Write(w *bufio.Writer) (err error) {
	var body []byte

	if _, err = w.WriteString("IDENTIFY\n"); err != nil {
		return
	}

	if body, err = json.Marshal(c.Info); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, int32(len(body))); err != nil {
		return
	}

	_, err = w.Write(body)
	return
}

func readIdentify(r *bufio.Reader, args ...string) (cmd Identify, err error) {
	var body []byte
	var size int32

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = makeErrBadBody("IDENTIFY failed to read body size")
		return
	}

	body = make([]byte, int(size))

	if _, err = io.ReadFull(r, body); err != nil {
		err = makeErrBadBody("IDENTIFY failed to read body")
		return
	}

	if err = json.Unmarshal(body, &cmd.Info); err != nil {
		err = makeErrBadBody("IDENTIFY failed to decode JSON body")
		return
	}

	if len(cmd.Info.BroadcastAddress) == 0 || cmd.Info.TcpPort == 0 || cmd.Info.HttpPort == 0 || len(cmd.Info.Version) == 0 {
		err = makeErrBadBody("IDENTIFY missing fields")
		return
	}

	return
}
