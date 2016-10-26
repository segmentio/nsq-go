package nsq

import "testing"

func TestError(t *testing.T) {
	for _, e := range [...]Error{
		ErrInvalid,
		ErrBadBody,
		ErrBadTopic,
		ErrBadChannel,
		ErrBadMessage,
		ErrPubFailed,
		ErrMPubFailed,
		ErrFinFailed,
		ErrReqFailed,
		ErrTouchFailed,
		ErrAuthFailed,
		ErrUnauthorized,
	} {
		t.Run(e.String(), func(t *testing.T) {
			testFrame(t, e)
		})
	}
}
