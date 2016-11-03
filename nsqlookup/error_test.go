package nsqlookup

import "testing"

func TestError(t *testing.T) {
	for _, err := range []Error{
		makeErrInvalid("A"),
		makeErrBadTopic("B"),
		makeErrBadChannel("C"),
		makeErrBadBody("D"),
	} {
		t.Run(err.Error(), func(t *testing.T) {
			testResponse(t, err.Code, err)
		})
	}
}
