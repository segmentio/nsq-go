package nsq

import (
	"testing"
	"time"
)

var (
	nsqlookup = []string{
		"127.0.0.1:4161", // nsqlookup-1
		"127.0.0.1:4163", // nsqlookup-2
		"127.0.0.1:4165", // nsqlookup-3
	}

	nsqd = []string{
		"127.0.0.1:4151", // nsqd-1
		"127.0.0.1:4153", // nsqd-2
		"127.0.0.1:4155", // nsqd-3
	}
)

func TestLookup(t *testing.T) {
	for _, node := range nsqd {
		c := &Client{Address: node}

		if err := c.CreateTopic("test-lookup"); err != nil {
			t.Error(err)
			return
		}

		// stack cleanup callbacks
		defer func() {
			if err := c.DeleteTopic("test-lookup"); err != nil {
				t.Error(err)
			}
		}()
	}

	// Allow some time for the nsqd nodes to inform nsqlookupd that they host
	// a specific topic.
	time.Sleep(100 * time.Millisecond)

	res, err := (&LookupClient{Addresses: nsqlookup}).Lookup("test-lookup")

	if err != nil {
		t.Error(err)
		return
	}

	if len(res.Channels) != 0 {
		t.Error("too many channels were reported by the lookup operation")

		for _, c := range res.Channels {
			t.Log(c)
		}
	}

	if len(res.Producers) != 3 {
		t.Error("not enough producers reported by the lookup operation")

		for _, p := range res.Producers {
			t.Logf("%#v", p)
		}
	}
}
