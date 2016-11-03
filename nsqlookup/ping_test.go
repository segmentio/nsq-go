package nsqlookup

import "testing"

func TestPing(t *testing.T) {
	testCommand(t, "PING", Ping{})
}
