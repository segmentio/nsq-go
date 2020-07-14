package nsq

import (
	"testing"
	"time"
)

func TestDPub(t *testing.T) {
	tests := []struct {
		topic   string
		delay   time.Duration
		message string
	}{
		{
			topic:   "A",
			delay:   0,
			message: "",
		},
		{
			topic:   "B",
			delay:   1 * time.Second,
			message: "Hello World!",
		},
	}

	for _, test := range tests {
		t.Run("topic:"+test.topic, func(t *testing.T) {
			testCommand(t, "DPUB", DPub{
				Topic:   test.topic,
				Delay:   test.delay,
				Message: []byte(test.message),
			})
		})
	}
}
