package nsq

import "testing"

func TestNop(t *testing.T) {
	testCommand(t, "NOP", Nop{})
}
