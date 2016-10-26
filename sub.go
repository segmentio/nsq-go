package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

type Sub struct {
	Topic   string
	Channel string
}

func (c Sub) Name() string {
	return "SUB"
}

func (c Sub) Write(w *bufio.Writer) (err error) {
	for _, s := range [...]string{
		"SUB ",
		c.Topic,
		" ",
		c.Channel,
		"\n",
	} {
		if _, err = w.WriteString(s); err != nil {
			err = errors.Wrap(err, "writing SUB command")
			return
		}
	}
	return
}

func readSub(line string) (cmd Sub, err error) {
	cmd.Topic, line = readNextWord(line)
	cmd.Channel, line = readNextWord(line)

	if len(cmd.Topic) == 0 {
		err = errors.New("missing topic in SUB command")
		return
	}

	if len(cmd.Channel) == 0 {
		err = errors.New("missing channel in SUB command")
		return
	}

	if len(line) != 0 {
		err = errors.New("too many arguments found in SUB command")
		return
	}

	return
}
