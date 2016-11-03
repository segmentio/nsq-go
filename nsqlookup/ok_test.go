package nsqlookup

import "testing"

func TestOK(t *testing.T) {
	testResponse(t, "OK", OK{})
}
