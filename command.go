package nsq

import (
	"bufio"
	"strings"

	"github.com/pkg/errors"
)

// The Command interface is implemented by types that represent the different
// commands of the NSQ protocol.
type Command interface {
	// Name returns the name of the command.
	Name() string

	// Write serializes the command to the given buffered output.
	Write(*bufio.Writer) error
}

// ReadCommand reads a command from the buffered input r, returning it or an
// error if something went wrong.
//
//	if cmd, err := nsq.ReadCommand(r); err != nil {
//		// handle the error
//		...
//	} else {
//		switch c := cmd.(type) {
//		case nsq.Pub:
//			...
//		}
//	}
//
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

	if strings.HasPrefix(line, "DPUB ") {
		return readDPub(line[5:], r)
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

func sendCommand(cmdChan chan<- Command, cmd Command) bool {
	defer func() { recover() }() // catch panics if the channel was already closed
	cmdChan <- cmd
	return true
}

func closeCommand(cmdChan chan<- Command) {
	defer func() { recover() }() // make the close operation idempotent
	close(cmdChan)
}
