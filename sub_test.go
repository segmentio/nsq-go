package nsq

import "testing"

func TestSub(t *testing.T) {
	tests := []struct {
		topic   string
		channel string
	}{
		{
			topic:   "A",
			channel: "B",
		},
		{
			topic:   "hello",
			channel: "world",
		},
	}

	for _, test := range tests {
		t.Run("topic:"+test.topic+",channel:"+test.channel, func(t *testing.T) {
			testCommand(t, "SUB", Sub{
				Topic:   test.topic,
				Channel: test.channel,
			})
		})
	}
}
