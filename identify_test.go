package nsq

import (
	"testing"
	"time"
)

func TestIdentify(t *testing.T) {
	testCommand(t, "IDENTIFY", Identify{
		ClientID:  "0123456789",
		Hostname:  "localhost",
		UserAgent: "nsq-go/test",
	})

	testCommand(t, "IDENTIFY", Identify{
		ClientID:   "0123456789",
		Hostname:   "localhost",
		UserAgent:  "nsq-go/test",
		MsgTimeout: time.Second * 5,
	})
}
