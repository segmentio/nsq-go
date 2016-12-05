package nsqlookup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"sort"
	"strconv"
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
	mutex   sync.RWMutex
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

		join := &sync.WaitGroup{}

		for _, sid := range list {
			join.Add(1)
			go func(sid string) {
				defer join.Done()
				e.destroySession(sid)
			}(sid)
		}

		join.Wait()
	})
	return
}

func (e *ConsulEngine) RegisterNode(node NodeInfo) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		k := httpBroadcastAddress(node)
		n := &consulNode{
			expTime: now.Add(e.nodeTimeout),
		}
		n.mutex.Lock()
		e.mutex.Lock()

		if n := e.nodes[k]; n != nil {
			if now.After(n.expTime) {
				err = errMissingNode
			} else {
				sid = n.sid
				err = n.err
			}
			e.mutex.Unlock()
			return
		}

		e.nodes[k] = n
		e.mutex.Unlock()

		if sid, err = e.createSession(e.nodeTimeout); err != nil {
			n.err = err
		} else {
			n.sid = sid
		}

		if err = e.registerNode(node, sid); err != nil {
			n.sid = ""
			n.err = err
		}

		n.mutex.Unlock()

		if err != nil {
			e.mutex.Lock()
			delete(e.nodes, k)
			e.mutex.Unlock()
		}
	}

	return
}

func (e *ConsulEngine) UnregisterNode(node NodeInfo) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) != 0 {
		e.mutex.Lock()
		delete(e.nodes, sid)
		e.mutex.Unlock()
		err = e.destroySession(sid)
	}

	return
}

func (e *ConsulEngine) PingNode(node NodeInfo) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
	} else {
		err = e.renewSession(sid)
	}

	return
}

func (e *ConsulEngine) TombstoneTopic(node NodeInfo, topic string) (err error) {
	var sid string
	var now = time.Now()
	var exp = now.Add(e.tombTimeout)

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
	} else {
		err = e.tombstoneTopic(node, topic, sid, exp)
	}

	return
}

func (e *ConsulEngine) RegisterTopic(node NodeInfo, topic string) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
	} else {
		err = e.registerTopic(node, topic, sid)
	}

	return
}

func (e *ConsulEngine) UnregisterTopic(node NodeInfo, topic string) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
	} else {
		err = e.unregisterTopic(node, topic)
	}

	if consulErrorNotFound(err) {
		err = nil
	}

	return
}

func (e *ConsulEngine) RegisterChannel(node NodeInfo, topic string, channel string) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
		return
	}

	if err = e.registerTopic(node, topic, sid); err != nil {
		return
	}

	err = e.registerChannel(node, topic, channel, sid)
	return
}

func (e *ConsulEngine) UnregisterChannel(node NodeInfo, topic string, channel string) (err error) {
	var sid string
	var now = time.Now()

	if sid, err = e.session(node, now); err != nil {
		return
	}

	if len(sid) == 0 {
		err = errMissingNode
	} else {
		err = e.unregisterChannel(node, topic, channel)
	}

	if consulErrorNotFound(err) {
		err = nil
	}

	return
}

func (e *ConsulEngine) LookupNodes() ([]NodeInfo, error) {
	return e.getNodes("nodes", time.Now())
}

func (e *ConsulEngine) LookupProducers(topic string) (producers []NodeInfo, err error) {
	now := time.Now()

	resChan1 := make(chan []NodeInfo)
	resChan2 := make(chan []NodeInfo)

	errChan1 := make(chan error)
	errChan2 := make(chan error)

	lookup := func(key string, res chan<- []NodeInfo, err chan<- error) {
		if n, e := e.getNodes(key, now); e != nil {
			err <- e
		} else {
			res <- n
		}
	}

	go lookup(path.Join("topics", topic, "nodes"), resChan1, errChan1)
	go lookup(path.Join("topics", topic, "tombs"), resChan2, errChan2)

	var nodes []NodeInfo
	var tombs []NodeInfo

	for i := 0; i != 2; i++ {
		select {
		case nodes = <-resChan1:
		case tombs = <-resChan2:
		case e := <-errChan1:
			err = appendError(err, e)
		case e := <-errChan2:
			err = appendError(err, e)
		}
	}

	if err != nil {
		return
	}

searchProducers:
	for _, n := range nodes {
		for _, t := range tombs {
			if t.BroadcastAddress == n.BroadcastAddress && t.HttpPort == n.HttpPort {
				continue searchProducers
			}
		}
		producers = append(producers, n)
	}

	return
}

func (e *ConsulEngine) LookupTopics() (topics []string, err error) {
	topics, err = e.listKeys("topics")

	if consulErrorNotFound(err) {
		err = nil
	}

	return
}

func (e *ConsulEngine) LookupChannels(topic string) (channels []string, err error) {
	channels, err = e.listKeys(path.Join("topics", topic, "channels"))

	if consulErrorNotFound(err) {
		err = nil
	}

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

func (e *ConsulEngine) session(node NodeInfo, now time.Time) (sid string, err error) {
	key := httpBroadcastAddress(node)
	e.mutex.RLock()
	if e.nodes == nil {
		err = io.ErrClosedPipe
	}
	n := e.nodes[key]
	e.mutex.RUnlock()

	if n != nil {
		n.mutex.RLock()
		if !now.After(n.expTime) {
			sid = n.sid
			err = n.err
		}
		n.mutex.RUnlock()
	}

	return
}

func (e *ConsulEngine) createSession(ttl time.Duration) (sid string, err error) {
	const minTTL = time.Second * 10
	const maxTTL = time.Second * 86400

	var session struct{ ID string }

	if ttl < minTTL {
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
		LockDelay: "0s",
		Name:      "nsqlookupd consul engine",
		Behavior:  "delete",
		TTL:       strconv.Itoa(int(ttl.Seconds())) + "s",
	}, &session); err != nil {
		return
	}

	sid = session.ID
	return
}

func (e *ConsulEngine) destroySession(sid string) error {
	return e.put("/v1/session/destroy/"+sid, nil, nil)
}

func (e *ConsulEngine) renewSession(sid string) error {
	return e.put("/v1/session/renew/"+sid, nil, nil)
}

func (e *ConsulEngine) registerNode(node NodeInfo, sid string) error {
	return e.setKey(consulNodeKey(node), sid, consulValue{Node: node})
}

func (e *ConsulEngine) registerTopic(node NodeInfo, topic string, sid string) error {
	return e.setKey(consulTopicKey(node, topic), sid, consulValue{Node: node})
}

func (e *ConsulEngine) unregisterTopic(node NodeInfo, topic string) error {
	return e.unsetKey(consulTopicKey(node, topic))
}

func (e *ConsulEngine) registerChannel(node NodeInfo, topic string, channel string, sid string) error {
	return e.setKey(consulChannelKey(node, topic, channel), sid, consulValue{Node: node})
}

func (e *ConsulEngine) unregisterChannel(node NodeInfo, topic string, channel string) error {
	return e.unsetKey(consulChannelKey(node, topic, channel))
}

func (e *ConsulEngine) tombstoneTopic(node NodeInfo, topic string, sid string, exp time.Time) error {
	return e.setKey(consulTombstoneKey(node, topic), sid, consulValue{Node: node, Deadline: &exp})
}

func (e *ConsulEngine) listKeys(prefix string) (keys []string, err error) {
	if err = e.get("/v1/kv/nsqlookup/"+prefix+"?keys", &keys); err != nil {
		return
	}

	cache := make(map[string]bool)
	prefix = "nsqlookup/" + prefix + "/"

	for _, k := range keys {
		cache[consulRootKey(k[len(prefix):])] = true
	}

	n := 0

	for k := range cache {
		keys[n] = k
		n++
	}

	keys = keys[:n]
	sort.Strings(keys)
	return
}

func (e *ConsulEngine) getNodes(key string, now time.Time) (nodes []NodeInfo, err error) {
	var values []consulValue

	if values, err = e.getKey(key); err != nil {
		if consulErrorNotFound(err) {
			err = nil
		}
		return
	}

	if len(values) != 0 {
		nodes = make([]NodeInfo, 0, len(values))

		for _, v := range values {
			if v.Deadline == nil || !now.After(*v.Deadline) {
				nodes = append(nodes, v.Node)
			}
		}

		if len(nodes) == 0 {
			nodes = nil
		}
	}

	return
}

func (e *ConsulEngine) getKey(key string) (values []consulValue, err error) {
	var kv []struct{ Value []byte }

	if err = e.get("/v1/kv/nsqlookup/"+key+"?recurse", &kv); err != nil {
		return
	}

	values = make([]consulValue, len(kv))

	for i, x := range kv {
		var v consulValue

		if err = json.Unmarshal(x.Value, &v); err != nil {
			values = nil
			return
		}

		values[i] = v
	}

	return
}

func (e *ConsulEngine) setKey(key string, sid string, value consulValue) (err error) {
	return e.put("/v1/kv/nsqlookup/"+key+"?acquire="+sid, value, nil)
}

func (e *ConsulEngine) unsetKey(key string) error {
	return e.delete("/v1/kv/nsqlookup/" + key + "?recurse")
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
		buf := bytes.Buffer{}
		buf.Grow(3 * len(b))
		if json.Indent(&buf, b, "", "  ") == nil {
			b = buf.Bytes()
		}
	}

	if req, err = http.NewRequest(method, e.address+path, bytes.NewReader(b)); err != nil {
		return
	}

	if res, err = e.client.Do(req); err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		io.Copy(ioutil.Discard, res.Body)
		err = consulError{
			method: method,
			path:   path,
			status: res.StatusCode,
			reason: res.Status,
		}
		return
	}

	if recv != nil {
		if err = json.NewDecoder(res.Body).Decode(recv); err != nil {
			return
		}
	} else {
		io.Copy(ioutil.Discard, res.Body)
	}

	return
}

func consulNodeKey(node NodeInfo) string {
	return path.Join("nodes", httpBroadcastAddress(node))
}

func consulTopicKey(node NodeInfo, topic string) string {
	return path.Join("topics", topic, "nodes", httpBroadcastAddress(node))
}

func consulChannelKey(node NodeInfo, topic string, channel string) string {
	return path.Join("topics", topic, "channels", channel, "nodes", httpBroadcastAddress(node))
}

func consulTombstoneKey(node NodeInfo, topic string) string {
	return path.Join("topics", topic, "tombstones", httpBroadcastAddress(node))
}

func consulRootKey(key string) string {
	if off := strings.IndexByte(key, '/'); off >= 0 {
		key = key[:off]
	}
	return key
}

func consulErrorNotFound(err error) bool {
	if err != nil {
		if e, ok := err.(consulError); ok {
			return e.status == http.StatusNotFound
		}
	}
	return false
}

type consulError struct {
	method string
	path   string
	status int
	reason string
}

func (e consulError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.method, e.path, e.reason)
}

type consulValue struct {
	Node     NodeInfo   `json:"nsqd"`
	Deadline *time.Time `json:"deadline,omitempty"`
}
