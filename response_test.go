package nsq

import "testing"

func TestResponse(t *testing.T) {
	for _, r := range [...]Response{
		OK,
		CloseWait,
	} {
		t.Run(r.String(), func(t *testing.T) {
			testFrame(t, r)
		})
	}
}
