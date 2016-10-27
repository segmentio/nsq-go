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

func Lookup(topic string, addrs ...string) (result LookupResult, err error) {
	n := len(addrs)

	if n == 0 {
		err = errors.New("missing addresses when looking up topic " + topic)
		return
	}

	resultList := make([]LookupResult, 0, n)
	resultChan := make(chan lookupResult, n)
	deadline := time.NewTimer(DefaultLookupTimeout)
	defer deadline.Stop()

	for _, addr := range addrs {
		go lookupAsync(topic, addr, resultChan)
	}

resultLoop:
	for i := 0; i != n; i++ {
		select {
		case r := <-resultChan:
			if r.error != nil {
				err = r.error
			} else {
				resultList = append(resultList, r.LookupResult)
			}

		case <-deadline.C:
			if i == 0 {
				err = errors.New("no response came back when looking up topic " + topic)
			}
			break resultLoop
		}
	}

	result = mergeLookupResults(resultList)
	return
}

func lookupAsync(topic string, addr string, resultChan chan<- lookupResult) {
	result, err := lookup(topic, addr)
	resultChan <- lookupResult{result, err}
}

func lookup(topic string, addr string) (result LookupResult, err error) {
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

	if v.StatusCode != 200 {
		err = errors.Errorf("looking for topic %s returned %d %s", topic, v.StatusCode, v.StatusTxt)
		return
	}

	result = v.Data
	return
}

func mergeLookupResults(resultList []LookupResult) (result LookupResult) {
	channels := make(map[string]bool)
	producers := make(map[ProducerInfo]bool)

	for _, r := range resultList {
		for _, c := range r.Channels {
			channels[c] = true
		}

		for _, p := range r.Producers {
			producers[p] = true
		}
	}

	result = LookupResult{
		Channels:  make([]string, 0, len(channels)),
		Producers: make([]ProducerInfo, 0, len(producers)),
	}

	for c := range channels {
		result.Channels = append(result.Channels, c)
	}

	for p := range producers {
		result.Producers = append(result.Producers, p)
	}

	return
}
