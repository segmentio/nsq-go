package nsq

import (
	"bufio"
	"strings"

	"github.com/pkg/errors"
)

type Command interface {
	Name() string

	write(*bufio.Writer) error
}

func ReadCommand(r *bufio.Reader) (cmd Command, err error) {
	var line string

	if line, err = r.ReadString('\n'); err != nil {
		err = errors.Wrap(err, "reading command")
		return
	}

	if n := len(line); n == 0 || line[n-1] != '\n' {
		err = errors.New("missing newline at the end of a command")
		return
	} else {
		line = line[:n-1]
	}

	if line == "IDENTIFY" {
		return readIdentify(r)
	}

	if strings.HasPrefix(line, "SUB ") {
		return readSub(line[4:])
	}

	if strings.HasPrefix(line, "PUB ") {
		return readPub(line[4:], r)
	}

	if strings.HasPrefix(line, "MPUB ") {
		return readMPub(line[5:], r)
	}

	if strings.HasPrefix(line, "RDY ") {
		return readRdy(line[4:])
	}

	if strings.HasPrefix(line, "FIN ") {
		return readFin(line[4:])
	}

	if strings.HasPrefix(line, "REQ ") {
		return readReq(line[4:])
	}

	if strings.HasPrefix(line, "TOUCH ") {
		return readTouch(line[6:])
	}

	if line == "AUTH" {
		return readAuth(r)
	}

	if line == "CLS" {
		cmd = Cls{}
		return
	}

	if line == "NOP" {
		cmd = Nop{}
		return
	}

	err = errors.New("unknown command " + line)
	return
}

func readNextWord(text string) (word string, next string) {
	if i := strings.IndexByte(text, ' '); i < 0 {
		word = text
	} else {
		word = text[:i]
		next = text[i+1:]
	}
	return
}
