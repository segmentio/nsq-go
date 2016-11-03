package nsqlookup

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func testResponse(t *testing.T, status string, r1 Response) {
	b := &bytes.Buffer{}
	r := bufio.NewReader(b)
	w := bufio.NewWriter(b)

	if s := r1.Status(); s != status {
		t.Error("status:", s, "!=", status)
		return
	}

	if err := r1.Write(w); err != nil {
		t.Errorf("write: %+v", err)
		return
	}

	if err := w.Flush(); err != nil {
		t.Errorf("flush: %+v", err)
		return
	}

	if r2, err := ReadResponse(r); err != nil {
		t.Errorf("read: %+v", err)
	} else if !reflect.DeepEqual(r1, r2) {
		t.Errorf("responses don't match:\n%#v\n%#v", r1, r2)
	}
}
