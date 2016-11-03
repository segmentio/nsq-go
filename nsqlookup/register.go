package nsqlookup

import (
	"bufio"
	"strings"
)

type Register struct {
	Topic   string
	Channel string
}

func (c Register) Name() string {
	return "REGISTER"
}

func (c Register) Write(w *bufio.Writer) (err error) {
	args := []string{c.Name(), c.Topic}

	if len(c.Channel) != 0 {
		args = append(args, c.Channel)
	}

	_, err = w.WriteString(strings.Join(args, " ") + "\n")
	return
}

func readRegister(args ...string) (cmd Register, err error) {
	cmd.Topic, cmd.Channel, err = getTopicChannel(cmd.Name(), args...)
	return
}
