package nsqlookup

import "testing"

func TestRegister(t *testing.T) {
	for _, test := range []Register{
		Register{
			Topic: "hello",
		},
		Register{
			Topic:   "hello",
			Channel: "world",
		},
	} {
		t.Run(test.Topic+":"+test.Channel, func(t *testing.T) {
			testCommand(t, "REGISTER", test)
		})
	}
}
