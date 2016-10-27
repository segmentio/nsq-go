package nsq

import (
	"bufio"

	"github.com/pkg/errors"
)

// Sub represents the SUB command.
type Sub struct {
	// Topic must be set to the name of the topic to subscribe to.
	Topic string

	// Channel must be set to the name of the channel to subscribe to.
	Channel string
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Sub) Name() string {
	return "SUB"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
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
