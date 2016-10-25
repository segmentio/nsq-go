package nsq

import (
	"strconv"
	"testing"
)

func TestTouch(t *testing.T) {
	tests := []struct {
		id MessageID
	}{
		{0},
		{1},
		{10},
		{42},
		{1234567890},
	}

	for _, test := range tests {
		t.Run("id:"+strconv.FormatUint(uint64(test.id), 16), func(t *testing.T) {
			testCommand(t, "TOUCH", Touch{
				MessageID: test.id,
			})
		})
	}
}
