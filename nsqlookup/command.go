package nsqlookup

import (
	"bufio"
	"encoding/binary"
	"regexp"
	"strings"
)

type Command interface {
	Name() string

	Write(w *bufio.Writer) error
}

func ReadCommand(r *bufio.Reader) (cmd Command, err error) {
	var line string

	if line, err = r.ReadString('\n'); err != nil {
		return
	}

	parts := strings.Split(strings.TrimSpace(line), " ")

	if len(parts) == 0 {
		err = makeErrInvalid("invalid empty command")
		return
	}

	switch name, args := parts[0], parts[1:]; name {
	case "PING":
		return readPing(args...)

	case "IDENTIFY":
		return readIdentify(r, args...)

	case "REGISTER":
		return readRegister(args...)

	case "UNREGISTER":
		return readUnregister(args...)

	default:
		err = makeErrInvalid("invalid command %s", name)
		return
	}
}

// =============================================================================
// These functions were adapted from https://github.com/nsqio/nsq

func writeResponse(w *bufio.Writer, b []byte) (err error) {
	if err = binary.Write(w, binary.BigEndian, int32(len(b))); err != nil {
		return
	}
	_, err = w.Write(b)
	return
}

func getTopicChannel(cmd string, args ...string) (topic string, channel string, err error) {
	if len(args) == 0 {
		err = makeErrInvalid("%s insufficient number of args", cmd)
		return
	}

	topic = args[0]

	if len(args) > 1 {
		channel = args[1]
	}

	if !isValidTopicName(topic) {
		err = makeErrBadTopic("%s topic name '%s' is not valid", cmd, topic)
	}

	if len(channel) != 0 && !isValidChannelName(channel) {
		err = makeErrBadChannel("%s channel name '%s' is not valid", cmd, channel)
	}

	if err != nil {
		topic, channel = "", ""
	}

	return
}

func isValidTopicName(name string) bool {
	return isValidName(name)
}

func isValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}

var (
	validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)
)

// =============================================================================
