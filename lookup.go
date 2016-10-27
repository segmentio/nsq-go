package nsq

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type ProducerInfo struct {
	BroadcastAddress string `json:"broadcast_address,omitempty"`
	RemoteAddress    string `json:"remote_address,omitempty"`
	Hostname         string `json:"hostname,omitempty"`
	Version          string `json:"version,omitempty"`
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
}

type LookupResult struct {
	Channels  []string       `json:"channels"`
	Producers []ProducerInfo `json:"producers"`
}

type lookupResult struct {
	LookupResult
	error
}

func Lookup(topic string, addrs ...string) (resp LookupResult, err error) {
	if len(addrs) == 0 {
		err = errors.New("missing addresses when looking up topic " + topic)
		return
	}

	respList := make([]LookupResult, 0, len(addrs))
	respChan := make(chan lookupResult, len(addrs))
	deadline := time.NewTimer(DefaultLookupTimeout)
	defer deadline.Stop()

	for _, addr := range addrs {
		go lookupAsync(topic, addr, respChan)
	}

respLoop:
	for i := 0; i != len(addrs); i++ {
		select {
		case r := <-respChan:
			if r.error != nil {
				err = r.error
			} else {
				respList = append(respList, r.LookupResult)
			}

		case <-deadline.C:
			if i == 0 {
				err = errors.New("no response came back when looking up topic " + topic)
			}
			break respLoop
		}
	}

	resp = mergeLookupResults(respList)
	return
}

func lookupAsync(topic string, addr string, respChan chan<- lookupResult) {
	resp, err := lookup(topic, addr)
	respChan <- lookupResult{resp, err}
}

func lookup(topic string, addr string) (resp LookupResult, err error) {
	var res *http.Response
	var dec *json.Decoder

	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}

	if res, err = http.Get(addr + "/lookup?topic=" + topic); err != nil {
		err = errors.Wrap(err, "looking up topic "+topic+" on http://"+addr)
		return
	}

	defer res.Body.Close()

	v := struct {
		StatusCode int          `json:"status_code"`
		StatusTxt  string       `json:"status_txt"`
		Data       LookupResult `json:"data"`
	}{}

	dec = json.NewDecoder(res.Body)
	err = dec.Decode(&v)

	resp = v.Data
	return
}

func mergeLookupResults(respList []LookupResult) (resp LookupResult) {
	channels := make(map[string]bool)
	producers := make(map[ProducerInfo]bool)

	for _, r := range respList {
		for _, c := range r.Channels {
			channels[c] = true
		}

		for _, p := range r.Producers {
			producers[p] = true
		}
	}

	resp = LookupResult{
		Channels:  make([]string, 0, len(channels)),
		Producers: make([]ProducerInfo, 0, len(producers)),
	}

	for c := range channels {
		resp.Channels = append(resp.Channels, c)
	}

	for p := range producers {
		resp.Producers = append(resp.Producers, p)
	}

	return
}
