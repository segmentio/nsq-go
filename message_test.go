package nsq

import (
	"testing"
	"time"
)

func TestMessage(t *testing.T) {
	for _, m := range [...]Message{
		Message{
			Body:      []byte{},
			Timestamp: time.Unix(0, 0),
		},
		Message{
			ID:        42,
			Attempts:  3,
			Body:      []byte("Hello World!"),
			Timestamp: time.Now().Round(time.Nanosecond),
		},
	} {
		t.Run(m.ID.String(), func(t *testing.T) {
			testFrame(t, m)
		})
	}
}
