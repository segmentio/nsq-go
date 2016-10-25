package nsq

import "testing"

func TestPub(t *testing.T) {
	tests := []struct {
		topic   string
		message string
	}{
		{
			topic:   "A",
			message: "",
		},
		{
			topic:   "B",
			message: "Hello World!",
		},
	}

	for _, test := range tests {
		t.Run("topic:"+test.topic, func(t *testing.T) {
			testCommand(t, "PUB", Pub{
				Topic:   test.topic,
				Message: []byte(test.message),
			})
		})
	}
}
