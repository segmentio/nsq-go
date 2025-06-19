package nsq

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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

type LookupClient struct {
	http.Client
	Addresses []string
	Scheme    string
	UserAgent string
}

func (c *LookupClient) Lookup(topic string) (result LookupResult, err error) {
	var retList [][]byte

	if retList, err = c.doAll("GET", "/lookup", url.Values{"topic": []string{topic}}, nil); len(retList) == 0 {
		return
	}

	type producerInfoKey struct {
		BroadcastAddress string
		Hostname         string
		Version          string
		TcpPort          int
		HttpPort         int
	}

	channels := make(map[string]bool)
	producers := make(map[producerInfoKey]ProducerInfo)

	for _, r := range retList {
		var lookupResult LookupResult

		if e := json.Unmarshal(r, &lookupResult); e != nil {
			err = appendError(err, e)
			continue
		}

		for _, c := range lookupResult.Channels {
			channels[c] = true
		}

		for _, p := range lookupResult.Producers {
			producers[producerInfoKey{
				BroadcastAddress: p.BroadcastAddress,
				Hostname:         p.Hostname,
				Version:          p.Version,
				TcpPort:          p.TcpPort,
				HttpPort:         p.HttpPort,
			}] = p
		}
	}

	result = LookupResult{
		Channels:  make([]string, 0, len(channels)),
		Producers: make([]ProducerInfo, 0, len(producers)),
	}

	for c := range channels {
		result.Channels = append(result.Channels, c)
	}

	for _, p := range producers {
		result.Producers = append(result.Producers, p)
	}

	return
}

func (c *LookupClient) doAll(method string, path string, query url.Values, data []byte) (ret [][]byte, err error) {
	addrs := c.Addresses

	if len(addrs) == 0 {
		addrs = []string{"localhost:4161"}
	}

	retChan := make(chan []byte, len(addrs))
	errChan := make(chan error, len(addrs))
	timeout := c.Client.Timeout

	if timeout == 0 {
		timeout = DefaultDialTimeout + DefaultReadTimeout + DefaultWriteTimeout
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for _, addr := range addrs {
		go c.doAsync(addr, method, path, query, data, retChan, errChan)
	}

	for i, n := 0, len(addrs); i != n; i++ {
		select {
		case r := <-retChan:
			ret = append(ret, r)

		case e := <-errChan:
			err = appendError(err, e)

		case <-deadline.C:
			err = appendError(err, errors.New("timeout"))
			return
		}
	}

	return
}

func (c *LookupClient) doAsync(host string, method string, path string, query url.Values, data []byte, ret chan<- []byte, err chan<- error) {
	if r, e := c.do(host, method, path, query, data); e != nil {
		err <- e
	} else {
		ret <- r
	}
}

func (c *LookupClient) do(host string, method string, path string, query url.Values, data []byte) (ret []byte, err error) {
	var res *http.Response
	var scheme = c.Scheme
	var userAgent = c.UserAgent
	var body io.ReadCloser

	if len(scheme) == 0 {
		scheme = "http"
	}

	if len(userAgent) == 0 {
		userAgent = DefaultUserAgent
	}

	if len(data) != 0 {
		body = io.NopCloser(bytes.NewReader(data))
	}

	if res, err = c.Do(&http.Request{
		Method: method,
		URL: &url.URL{
			Scheme:   scheme,
			Host:     host,
			Path:     path,
			RawQuery: query.Encode(),
		},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type": []string{"application/octet-stream"},
			"User-Agent":   []string{userAgent},
		},
		Body:          body,
		Host:          host,
		ContentLength: int64(len(data)),
	}); err != nil {
		err = errors.Wrapf(err, "%s %s://%s?%s", method, scheme, host, query.Encode())
		return
	}

	defer res.Body.Close()

	if ret, err = io.ReadAll(res.Body); err != nil {
		err = errors.Wrap(err, "reading response body")
		return
	}

	if res.StatusCode >= 400 {
		res.Body.Close()
		err = errors.Errorf("%s %s://%s?%s: %s %s", method, scheme, host, query.Encode(), res.Status, string(ret))
		return
	}

	return
}
