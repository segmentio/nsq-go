package nsq

import (
	"math"
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
			Timestamp: time.Unix(math.MaxUint32, 0),
		},
	} {
		t.Run(m.ID.String(), func(t *testing.T) {
			testFrame(t, m)
		})
	}
}
