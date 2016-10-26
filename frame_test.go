package nsq

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func testFrame(t *testing.T, f1 Frame) {
	b := &bytes.Buffer{}
	r := bufio.NewReader(b)
	w := bufio.NewWriter(b)

	if err := f1.Write(w); err != nil {
		t.Errorf("write: %+v", err)
		return
	}

	if err := w.Flush(); err != nil {
		t.Errorf("flush: %+v", err)
		return
	}

	if f2, err := ReadFrame(r); err != nil {
		t.Errorf("read: %+v", err)
	} else if !reflect.DeepEqual(f1, f2) {
		t.Errorf("frames don't match:\n%#v\n%#v", f1, f2)
	}
}
