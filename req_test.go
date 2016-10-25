package nsq

import (
	"strconv"
	"testing"
	"time"
)

func TestReq(t *testing.T) {
	tests := []struct {
		id      MessageID
		timeout time.Duration
	}{
		{
			id:      0,
			timeout: 0,
		},
		{
			id:      1,
			timeout: 1 * time.Second,
		},
	}

	for _, test := range tests {
		t.Run("id:"+strconv.FormatUint(uint64(test.id), 16), func(t *testing.T) {
			testCommand(t, "REQ", Req{
				MessageID: test.id,
			})
		})
	}
}
