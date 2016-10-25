package nsq

import (
	"strconv"
	"testing"
)

func TestRdy(t *testing.T) {
	tests := []struct {
		count int
	}{
		{0},
		{1},
		{10},
		{42},
		{1234567890},
	}

	for _, test := range tests {
		t.Run("count:"+strconv.Itoa(test.count), func(t *testing.T) {
			testCommand(t, "RDY", Rdy{
				Count: test.count,
			})
		})
	}
}
