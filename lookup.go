package nsq

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type LookupProducer struct {
	BroadcastAddress string `json:"broadcast_address,omitempty"`
	RemoteAddress    string `json:"remote_address,omitempty"`
	Hostname         string `json:"hostname,omitempty"`
	Version          string `json:"version,omitempty"`
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
}

type LookupTopicResponse struct {
	Channels  []string         `json:"channels"`
	Producers []LookupProducer `json:"producers"`
}

type lookupTopicResponse struct {
	LookupTopicResponse
	error
}

func LookupTopic(topic string, addrs ...string) (resp LookupTopicResponse, err error) {
	if len(addrs) == 0 {
		err = errors.New("missing addresses when looking up topic " + topic)
		return
	}

	respList := make([]LookupTopicResponse, 0, len(addrs))
	respChan := make(chan lookupTopicResponse, len(addrs))
	deadline := time.NewTimer(DefaultLookupTimeout)
	defer deadline.Stop()

	for _, addr := range addrs {
		go lookupTopicAsync(topic, addr, respChan)
	}

respLoop:
	for i := 0; i != len(addrs); i++ {
		select {
		case r := <-respChan:
			if r.error != nil {
				err = r.error
			} else {
				respList = append(respList, r.LookupTopicResponse)
			}

		case <-deadline.C:
			if i == 0 {
				err = errors.New("no response came back when looking up topic " + topic)
			}
			break respLoop
		}
	}

	resp = mergeLookupTopicResponses(respList)
	return
}

func lookupTopicAsync(topic string, addr string, respChan chan<- lookupTopicResponse) {
	resp, err := lookupTopic(topic, addr)
	respChan <- lookupTopicResponse{resp, err}
}

func lookupTopic(topic string, addr string) (resp LookupTopicResponse, err error) {
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
		StatusCode int                 `json:"status_code"`
		StatusTxt  string              `json:"status_txt"`
		Data       LookupTopicResponse `json:"data"`
	}{}

	dec = json.NewDecoder(res.Body)
	err = dec.Decode(&v)

	resp = v.Data
	return
}

func mergeLookupTopicResponses(respList []LookupTopicResponse) (resp LookupTopicResponse) {
	channels := make(map[string]bool)
	producers := make(map[LookupProducer]bool)

	for _, r := range respList {
		for _, c := range r.Channels {
			channels[c] = true
		}

		for _, p := range r.Producers {
			producers[p] = true
		}
	}

	resp = LookupTopicResponse{
		Channels:  make([]string, 0, len(channels)),
		Producers: make([]LookupProducer, 0, len(producers)),
	}

	for c := range channels {
		resp.Channels = append(resp.Channels, c)
	}

	for p := range producers {
		resp.Producers = append(resp.Producers, p)
	}

	return
}
