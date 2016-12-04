package nsqlookup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	DefaultConsulAddress  = "localhost:8500"
	DefaultRequestTimeout = 10 * time.Second
)

type ConsulConfig struct {
	Address          string
	NodeTimeout      time.Duration
	TombstoneTimeout time.Duration
	RequestTimeout   time.Duration
	Transport        http.RoundTripper
}

type ConsulEngine struct {
	client      http.Client
	address     string
	nodeTimeout time.Duration
	tombTimeout time.Duration

	once  sync.Once
	mutex sync.RWMutex
	nodes map[string]*consulNode
}

type consulNode struct {
	mutex   sync.Mutex
	sid     string
	err     error
	expTime time.Time
}

func NewConsulEngine(config ConsulConfig) *ConsulEngine {
	if len(config.Address) == 0 {
		config.Address = DefaultConsulAddress
	}

	if config.NodeTimeout == 0 {
		config.NodeTimeout = DefaultLocalEngineNodeTimeout
	}

	if config.TombstoneTimeout == 0 {
		config.TombstoneTimeout = DefaultLocalEngineTombstoneTimeout
	}

	if config.RequestTimeout == 0 {
		config.RequestTimeout = DefaultRequestTimeout
	}

	if !strings.Contains(config.Address, "://") {
		config.Address = "http://" + config.Address
	}

	return &ConsulEngine{
		client: http.Client{
			Transport: config.Transport,
			Timeout:   config.RequestTimeout,
		},
		address:     config.Address,
		nodeTimeout: config.NodeTimeout,
		tombTimeout: config.TombstoneTimeout,
		nodes:       make(map[string]*consulNode),
	}
}

func (e *ConsulEngine) Close() (err error) {
	e.once.Do(func() {
		e.mutex.Lock()
		list := make([]string, 0, len(e.nodes))

		for _, node := range e.nodes {
			node.mutex.Lock()
			if sid := node.sid; len(sid) != 0 {
				list = append(list, sid)
			}
			node.mutex.Unlock()
		}

		e.nodes = nil
		e.mutex.Unlock()

		g := &sync.WaitGroup{}

		for _, sid := range list {
			g.Add(1)
			go func(sid string) {
				e.destroySession(sid)
				g.Done()
			}(sid)
		}

		g.Wait()
	})
	return
}

func (e *ConsulEngine) RegisterNode(node NodeInfo) (err error) {

	return
}

func (e *ConsulEngine) UnregisterNode(node NodeInfo) (err error) {

	return
}

func (e *ConsulEngine) PingNode(node NodeInfo) (err error) {

	return
}

func (e *ConsulEngine) TombstoneTopic(node NodeInfo, topic string) (err error) {

	return
}

func (e *ConsulEngine) RegisterTopic(node NodeInfo, topic string) (err error) {

	return
}

func (e *ConsulEngine) UnregisterTopic(node NodeInfo, topic string) (err error) {

	return
}

func (e *ConsulEngine) RegisterChannel(node NodeInfo, topic string, channel string) (err error) {

	return
}

func (e *ConsulEngine) UnregisterChannel(node NodeInfo, topic string, channel string) (err error) {

	return
}

func (e *ConsulEngine) LookupNodes() (nodes []NodeInfo, err error) {

	return
}

func (e *ConsulEngine) LookupProducers(topic string) (producers []NodeInfo, err error) {

	return
}

func (e *ConsulEngine) LookupTopics() (topics []string, err error) {

	return
}

func (e *ConsulEngine) LookupChannels(topic string) (channels []string, err error) {

	return
}

func (e *ConsulEngine) LookupInfo() (info EngineInfo, err error) {
	info.Type = "consul"
	info.Version = "0.3.8"
	return
}

func (e *ConsulEngine) CheckHealth() (err error) {

	return
}

func (e *ConsulEngine) session(node string) (sid string, err error) {
	e.mutex.RLock()

	if e.nodes == nil {
		err = io.ErrClosedPipe
	} else if n := e.nodes[node]; n != nil {
		sid = n.sid
		err = n.err
	}

	e.mutex.RUnlock()

	if len(sid) == 0 && err == nil {
		n := &consulNode{}
		n.mutex.Lock()
		e.mutex.Lock()

		if n := e.nodes[node]; n != nil {
			sid = n.sid
			err = n.err
			e.mutex.Unlock()
			return
		}

		e.nodes[node] = n
		e.mutex.Unlock()

		if sid, err = e.createSession(); err != nil {
			n.err = err
		} else {
			n.sid = sid
		}

		n.mutex.Unlock()

		if err != nil {
			e.mutex.Lock()
			delete(e.nodes, node)
			e.mutex.Unlock()
		}
	}

	return
}

func (e *ConsulEngine) createSession() (sid string, err error) {
	const minTTL = time.Second * 10
	const maxTTL = time.Second * 86400

	var session struct{ ID string }
	var ttl time.Duration

	if ttl = e.nodeTimeout; ttl < minTTL {
		ttl = minTTL
	} else if ttl > maxTTL {
		ttl = maxTTL
	}

	if err = e.put("/v1/session/create", struct {
		LockDelay string
		Name      string
		Behavior  string
		TTL       string
	}{
		LockDelay: "15s",
		Name:      "nsqlookupd consul engine",
		Behavior:  "delete",
		TTL:       fmt.Sprint("%ds", int(ttl.Seconds())),
	}, &session); err != nil {
		return
	}

	sid = session.ID
	return
}

func (e *ConsulEngine) destroySession(sid string) error {
	return e.delete("/v1/session/destroy/" + sid)
}

func (e *ConsulEngine) renewSession(sid string) error {
	return e.put("/v1/session/renew/"+sid, nil, nil)
}

func (e *ConsulEngine) get(path string, recv interface{}) error {
	return e.do("GET", path, nil, recv)
}

func (e *ConsulEngine) put(path string, send interface{}, recv interface{}) error {
	return e.do("PUT", path, send, recv)
}

func (e *ConsulEngine) delete(path string) error {
	return e.do("DELETE", path, nil, nil)
}

func (e *ConsulEngine) do(method string, path string, send interface{}, recv interface{}) (err error) {
	var req *http.Request
	var res *http.Response
	var b []byte

	if send != nil {
		if b, err = json.Marshal(send); err != nil {
			return
		}
	}

	if req, err = http.NewRequest(method, path, bytes.NewReader(b)); err != nil {
		return
	}

	if res, err = e.client.Do(req); err != nil {
		return
	}
	defer res.Body.Close()

	if recv != nil {
		if err = json.NewDecoder(res.Body).Decode(recv); err != nil {
			return
		}
	} else {
		io.Copy(ioutil.Discard, res.Body)
	}

	return
}
