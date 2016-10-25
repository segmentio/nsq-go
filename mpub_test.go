package nsq

import "testing"

func TestMPub(t *testing.T) {
	tests := []struct {
		topic    string
		messages [][]byte
	}{
		{
			topic:    "A",
			messages: [][]byte{},
		},
		{
			topic: "B",
			messages: [][]byte{
				[]byte("Hello World!"),
			},
		},
		{
			topic: "C",
			messages: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
		},
	}

	for _, test := range tests {
		t.Run("topic:"+test.topic, func(t *testing.T) {
			testCommand(t, "MPUB", MPub{
				Topic:    test.topic,
				Messages: test.messages,
			})
		})
	}
}
