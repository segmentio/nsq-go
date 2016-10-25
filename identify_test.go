package nsq

import "testing"

func TestIdentify(t *testing.T) {
	testCommand(t, "IDENTIFY", Identify{
		ClientID:  "0123456789",
		Hostname:  "localhost",
		UserAgent: "nsq-go/test",
	})
}
