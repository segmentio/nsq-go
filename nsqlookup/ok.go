package nsqlookup

import "bufio"

type OK struct {
}

func (OK) Status() string {
	return "OK"
}

func (OK) Write(w *bufio.Writer) error {
	return writeResponse(w, []byte("OK"))
}
