package nsqlookup

import "bufio"

type Ping struct {
}

func (c Ping) Name() string {
	return "PING"
}

func (c Ping) Write(w *bufio.Writer) (err error) {
	_, err = w.WriteString("PING\n")
	return
}

func readPing(args ...string) (cmd Ping, err error) {
	return
}
