package nsqlookup

import "testing"

func TestUnregister(t *testing.T) {
	for _, test := range []Unregister{
		Unregister{
			Topic: "hello",
		},
		Unregister{
			Topic:   "hello",
			Channel: "world",
		},
	} {
		t.Run(test.Topic+":"+test.Channel, func(t *testing.T) {
			testCommand(t, "UNREGISTER", test)
		})
	}
}
