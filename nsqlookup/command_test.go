package nsqlookup

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func testCommand(t *testing.T, name string, c1 Command) {
	b := &bytes.Buffer{}
	r := bufio.NewReader(b)
	w := bufio.NewWriter(b)

	if s := c1.Name(); s != name {
		t.Error("name:", s, "!=", name)
		return
	}

	if err := c1.Write(w); err != nil {
		t.Errorf("write: %+v", err)
		return
	}

	if err := w.Flush(); err != nil {
		t.Errorf("flush: %+v", err)
		return
	}

	if c2, err := ReadCommand(r); err != nil {
		t.Errorf("read: %+v", err)
	} else if !reflect.DeepEqual(c1, c2) {
		t.Errorf("commands don't match:\n%#v\n%#v", c1, c2)
	}
}
