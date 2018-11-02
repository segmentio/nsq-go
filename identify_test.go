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
		// timeout omitted.
	})
	testCommand(t, "IDENTIFY", Identify{
		ClientID:       "0123456789",
		Hostname:       "localhost",
		UserAgent:      "nsq-go/test",
		MessageTimeout: 10 * time.Minute,
	})
}
