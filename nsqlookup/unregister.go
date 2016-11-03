package nsqlookup

import (
	"bufio"
	"strings"
)

type Unregister struct {
	Topic   string
	Channel string
}

func (c Unregister) Name() string {
	return "UNREGISTER"
}

func (c Unregister) Write(w *bufio.Writer) (err error) {
	args := []string{c.Name(), c.Topic}

	if len(c.Channel) != 0 {
		args = append(args, c.Channel)
	}

	_, err = w.WriteString(strings.Join(args, " ") + "\n")
	return
}

func readUnregister(args ...string) (cmd Unregister, err error) {
	cmd.Topic, cmd.Channel, err = getTopicChannel(cmd.Name(), args...)
	return
}
