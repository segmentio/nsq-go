package nsq

import "testing"

func TestCls(t *testing.T) {
	testCommand(t, "CLS", Cls{})
}
