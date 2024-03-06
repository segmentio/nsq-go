package nsqlookup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// A ProxyEngine implements the Engine interface an is intended to be used as a
// frontend to a set of standard nsqlookupd servers to expose them as if
// they were a single entity.
type ProxyEngine struct {
	Transport http.RoundTripper
	Topology  Topology
	Registry  Registry
	// Name of the nsqlookupd server, defaults to "nsqlookupd".
	Nsqlookupd string
	// List of topics for which the proxy applies zone restrictions of consumers
	// and producers.
	//
	// The value may be a magic ["*"] to indicate that the proxy should apply
	// zone awareness to all topics.
	ZoneAwareTopics []string
}

func (p *ProxyEngine) nsqlookupd() string {
	if p.Nsqlookupd != "" {
		return p.Nsqlookupd
	}
	return "nsqlookupd"
}

func (p *ProxyEngine) lookup(ctx context.Context) ([]string, error) {
	if p.Registry == nil {
		return nil, ctx.Err()
	}
	addrs, _, err := p.Registry.Lookup(ctx, p.nsqlookupd())
	return addrs, err
}

func (p *ProxyEngine) Close() error {
	return nil
}

func (p *ProxyEngine) RegisterNode(ctx context.Context, node NodeInfo) (Node, error) {
	return nil, errUnsupported
}

func (p *ProxyEngine) TombstoneTopic(ctx context.Context, node NodeInfo, topic string) (err error) {
	var servers []string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	srvcount := len(servers)
	errcount := 0
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			errors <- p.post(ctx, server+"/tombstone_topic_producer?topic="+url.QueryEscape(topic)+"&node="+url.QueryEscape(httpBroadcastAddress(node)), nil, nil)
		}(server)
	}

	for i := 0; i != srvcount; i++ {
		if e := <-errors; e != nil {
			err = appendError(err, e)
			errcount++
		}
	}

	if errcount == srvcount {
		return
	}

	err = nil
	return
}

func (p *ProxyEngine) LookupNodes(ctx context.Context) (nodes []NodeInfo2, err error) {
	var servers []string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	srvcount := len(servers)
	results := make(chan []NodeInfo2, srvcount)
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			var res struct {
				Producers []NodeInfo2 `json:"producers"`
			}
			if err := p.get(ctx, server+"/nodes", &res); err != nil {
				errors <- err
			} else {
				results <- res.Producers
			}
		}(server)
	}

	set := make(map[string]NodeInfo2)
	for i := 0; i != srvcount; i++ {
		select {
		case r := <-results:
			for _, n := range r {
				set[makeBroadcastAddress(n.BroadcastAddress, n.HttpPort)] = n
			}
		case e := <-errors:
			if e != errNotFound {
				err = appendError(err, e)
			}
		}
	}

	if len(set) != 0 {
		nodes = make([]NodeInfo2, 0, len(set))
		for _, node := range set {
			nodes = append(nodes, node)
		}
		err = nil
	}

	return
}

func (p *ProxyEngine) LookupProducers(ctx context.Context, topic string) (nodes []NodeInfo, err error) {
	var clientIP = ClientIP(ctx)
	var servers []string
	var inZone string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	if clientIP != nil && p.Topology != nil {
		zoneAware := len(p.ZoneAwareTopics) == 1 && p.ZoneAwareTopics[0] == "*"

		if !zoneAware {
			for _, zoneAwareTopic := range p.ZoneAwareTopics {
				if zoneAwareTopic == topic {
					zoneAware = true
					break
				}
			}
		}

		if zoneAware {
			// Ignore error here, we're better off returning servers out of zone
			// than preventing the consumers from discovering any nsqd servers.
			inZone, _ = p.Topology.LookupIPZone(ctx, clientIP)
		}
	}

	srvcount := len(servers)
	results := make(chan []NodeInfo, srvcount)
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			var res struct {
				Producers []NodeInfo `json:"producers"`
			}
			if err := p.get(ctx, server+"/lookup?topic="+url.QueryEscape(topic), &res); err != nil {
				errors <- err
			} else {
				results <- res.Producers
			}
		}(server)
	}

	set := make(map[string]NodeInfo)
	for i := 0; i != srvcount; i++ {
		select {
		case r := <-results:
			for _, n := range r {
				// When a client IP was specified and a topology configured on
				// the proxy, check whether the address of the nsqd server is in
				// the same zone as the client, and otherwise don't include it.
				//
				// Here again, the check defaults to including the nsqd server
				// to avoid cascading failures where a zone lookup error
				// prevents consumers from discovering nsqd servers that they
				// should have been consuming from.
				if inZone != "" {
					if ip := net.ParseIP(n.BroadcastAddress); ip != nil {
						if zone, _ := p.Topology.LookupIPZone(ctx, ip); zone != "" && zone != inZone {
							continue
						}
					}
				}
				set[httpBroadcastAddress(n)] = n
			}
		case e := <-errors:
			if e != errNotFound {
				err = appendError(err, e)
			}
		}
	}

	if len(set) != 0 {
		nodes = make([]NodeInfo, 0, len(set))
		for _, node := range set {
			nodes = append(nodes, node)
		}
		err = nil
	}

	return
}

func (p *ProxyEngine) LookupTopics(ctx context.Context) (topics []string, err error) {
	var servers []string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	srvcount := len(servers)
	results := make(chan []string, srvcount)
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			var res struct {
				Topics []string `json:"topics"`
			}
			if err := p.get(ctx, server+"/topics", &res); err != nil {
				errors <- err
			} else {
				results <- res.Topics
			}
		}(server)
	}

	set := make(map[string]bool)
	for i := 0; i != srvcount; i++ {
		select {
		case r := <-results:
			for _, c := range r {
				set[c] = true
			}
		case e := <-errors:
			if e != errNotFound {
				err = appendError(err, e)
			}
		}
	}

	if len(set) != 0 {
		topics = make([]string, 0, len(set))
		for topic := range set {
			topics = append(topics, topic)
		}
		err = nil
	}

	return
}

func (p *ProxyEngine) LookupChannels(ctx context.Context, topic string) (channels []string, err error) {
	var servers []string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	srvcount := len(servers)
	results := make(chan []string, srvcount)
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			var res struct {
				Channels []string `json:"channels"`
			}
			if err := p.get(ctx, server+"/channels?topic="+url.QueryEscape(topic), &res); err != nil {
				errors <- err
			} else {
				results <- res.Channels
			}
		}(server)
	}

	set := make(map[string]bool)
	for i := 0; i != srvcount; i++ {
		select {
		case r := <-results:
			for _, c := range r {
				set[c] = true
			}
		case e := <-errors:
			if e != errNotFound {
				err = appendError(err, e)
			}
		}
	}

	if len(set) != 0 {
		channels = make([]string, 0, len(set))
		for channel := range set {
			channels = append(channels, channel)
		}
		err = nil
	}

	return
}

func (p *ProxyEngine) LookupInfo(ctx context.Context) (info EngineInfo, err error) {
	info.Type = "proxy"
	info.Version = "0.3.8"
	return
}

func (p *ProxyEngine) CheckHealth(ctx context.Context) (err error) {
	var servers []string

	if servers, err = p.lookup(ctx); err != nil {
		return
	}

	srvcount := len(servers)
	errcount := 0
	errors := make(chan error, srvcount)

	for _, server := range servers {
		go func(server string) {
			errors <- p.get(ctx, server+"/ping", nil)
		}(server)
	}

	for i := 0; i != srvcount; i++ {
		if e := <-errors; e != nil {
			err = appendError(err, e)
			errcount++
		}
	}

	if errcount == srvcount {
		return
	}

	err = nil
	return
}

func (p *ProxyEngine) get(ctx context.Context, url string, recv interface{}) error {
	return p.do(ctx, "GET", url, nil, recv)
}

func (p *ProxyEngine) post(ctx context.Context, url string, send interface{}, recv interface{}) error {
	return p.do(ctx, "POST", url, send, recv)
}

func (e *ProxyEngine) do(ctx context.Context, method string, url string, send interface{}, recv interface{}) (err error) {
	var req *http.Request
	var res *http.Response
	var r io.Reader
	var b []byte
	var t http.RoundTripper

	if t = e.Transport; t == nil {
		t = http.DefaultTransport
	}

	if send != nil {
		if b, err = json.Marshal(send); err != nil {
			return
		}
		r = bytes.NewReader(b)
	}

	if !strings.Contains(url, "://") {
		url = "http://" + url
	}

	if req, err = http.NewRequest(method, url, r); err != nil {
		return
	}
	req.Header.Set("User-Agent", "nsqlookup proxy engine")
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	if res, err = t.RoundTrip(req); err != nil {
		return
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		io.Copy(io.Discard, res.Body)
		err = errNotFound
		return
	default:
		b := &bytes.Buffer{}
		io.Copy(b, res.Body)
		err = &proxyError{
			method:  method,
			url:     url,
			status:  res.StatusCode,
			reason:  res.Status,
			message: b.String(),
		}
		return
	}

	if recv == nil {
		io.Copy(io.Discard, res.Body)
		return
	}

	err = json.NewDecoder(res.Body).Decode(recv)
	return
}

type ProxyNode struct {
}

type proxyError struct {
	method  string
	url     string
	status  int
	reason  string
	message string
}

func (e *proxyError) Error() string {
	return fmt.Sprintf("%s %s: %d %s: %s", e.method, e.url, e.status, e.reason, e.message)
}

var (
	errNotFound    = errors.New("not found")
	errUnsupported = errors.New("unsupported")
)
