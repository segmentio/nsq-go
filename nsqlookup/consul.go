package nsqlookup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultConsulAddress is the default address at which a consul agent is
	// expected to be available for consul engines.
	DefaultConsulAddress = "localhost:8500"

	// DefaultConsulNamespace is the key namespace used by default by the consul
	// engine.
	DefaultConsulNamespace = "nsqlookup"
)

// The ConsulConfig structure is used to configure consul engines.
type ConsulConfig struct {
	// The address at which the consul agent is exposing its HTTP API.
	Address string

	// The namespace that the engine will be working on within the consul
	// key/value store.
	Namespace string

	// NodeTImeout is the maximum amount of time a node is allowed to be idle
	// before it gets evicted.
	NodeTimeout time.Duration

	// TomstoneTimeout is the amount of time after which a tombstone set on a
	// topic is evisted.
	TombstoneTimeout time.Duration

	// Transport used by the engine's HTTP client, the default transport is used
	// if none is provided.
	Transport http.RoundTripper
}

// ConsulEngine are objects that provide the implementation of a nsqlookup
// engine backed by a consul infrastructure.
type ConsulEngine struct {
	client      http.Client
	address     string
	namespace   string
	nodeTimeout time.Duration
	tombTimeout time.Duration
}

// NewConsulEngine creates and return a new engine configured with config.
func NewConsulEngine(config ConsulConfig) *ConsulEngine {
	if len(config.Address) == 0 {
		config.Address = DefaultConsulAddress
	}

	if len(config.Namespace) == 0 {
		config.Namespace = DefaultConsulNamespace
	}

	if config.NodeTimeout == 0 {
		config.NodeTimeout = DefaultLocalEngineNodeTimeout
	}

	if config.TombstoneTimeout == 0 {
		config.TombstoneTimeout = DefaultLocalEngineTombstoneTimeout
	}

	if !strings.Contains(config.Address, "://") {
		config.Address = "http://" + config.Address
	}

	return &ConsulEngine{
		client:      http.Client{Transport: config.Transport},
		address:     config.Address,
		namespace:   config.Namespace,
		nodeTimeout: config.NodeTimeout,
		tombTimeout: config.TombstoneTimeout,
	}
}

func (e *ConsulEngine) Close() (err error) {
	return
}

func (e *ConsulEngine) RegisterNode(ctx context.Context, node NodeInfo) (n Node, err error) {
	var sid string

	if sid, err = e.createSession(ctx, "nsqlookup.node:"+node.Hostname, e.nodeTimeout); err != nil {
		return
	}

	if err = e.registerNode(ctx, node, sid); err != nil {
		return
	}

	n = newConsulNode(e, sid, node)
	return
}

func (e *ConsulEngine) TombstoneTopic(ctx context.Context, node NodeInfo, topic string) (err error) {
	var sid string
	var now = time.Now()
	var exp = now.Add(e.tombTimeout)

	// Create a new session to manage the tombstone key's timeout independently.
	if sid, err = e.createSession(ctx, "nsqlookup.tombstone:"+topic, e.tombTimeout); err != nil {
		return
	}

	err = e.tombstoneTopic(ctx, node, topic, sid, exp.UTC())
	return
}

func (e *ConsulEngine) LookupNodes(ctx context.Context) ([]NodeInfo2, error) {
	nodes1, err := e.getNodes(ctx, "nodes", time.Now())
	if err != nil {
		return nil, err
	}

	nodes2 := make([]NodeInfo2, 0, len(nodes1))

	for _, n := range nodes1 {
		nodes2 = append(nodes2, NodeInfo2{
			RemoteAddress:    n.RemoteAddress,
			Hostname:         n.Hostname,
			BroadcastAddress: n.BroadcastAddress,
			TcpPort:          n.TcpPort,
			HttpPort:         n.HttpPort,
			Version:          n.Version,
			// TODO:
			//Tombstones: ...,
			//Topics: ...,
		})
	}

	return nodes2, nil
}

func (e *ConsulEngine) LookupProducers(ctx context.Context, topic string) (producers []NodeInfo, err error) {
	now := time.Now()

	resChan1 := make(chan []NodeInfo)
	resChan2 := make(chan []NodeInfo)

	errChan1 := make(chan error)
	errChan2 := make(chan error)

	lookup := func(key string, res chan<- []NodeInfo, err chan<- error) {
		if n, e := e.getNodes(ctx, key, now); e != nil {
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

func (e *ConsulEngine) LookupTopics(ctx context.Context) (topics []string, err error) {
	topics, err = e.listKeys(ctx, "topics")

	if consulErrorNotFound(err) {
		err = nil
	}

	return
}

func (e *ConsulEngine) LookupChannels(ctx context.Context, topic string) (channels []string, err error) {
	channels, err = e.listKeys(ctx, path.Join("topics", topic, "channels"))

	if consulErrorNotFound(err) {
		err = nil
	}

	return
}

func (e *ConsulEngine) LookupInfo(ctx context.Context) (info EngineInfo, err error) {
	info.Type = "consul"
	info.Version = "0.3.8"
	return
}

func (e *ConsulEngine) CheckHealth(ctx context.Context) error {
	return e.get(ctx, "/v1/agent/self", nil)
}

func (e *ConsulEngine) createSession(ctx context.Context, name string, ttl time.Duration) (sid string, err error) {
	const minTTL = time.Second * 10
	const maxTTL = time.Second * 86400

	var session struct{ ID string }

	if ttl < minTTL {
		ttl = minTTL
	} else if ttl > maxTTL {
		ttl = maxTTL
	}

	if err = e.put(ctx, "/v1/session/create", struct {
		LockDelay string
		Name      string
		Behavior  string
		TTL       string
	}{
		LockDelay: "0s",
		Name:      name,
		Behavior:  "delete",
		TTL:       strconv.Itoa(int(ttl.Seconds())) + "s",
	}, &session); err != nil {
		return
	}

	sid = session.ID
	return
}

func (e *ConsulEngine) destroySession(ctx context.Context, sid string) error {
	return e.put(ctx, "/v1/session/destroy/"+sid, nil, nil)
}

func (e *ConsulEngine) renewSession(ctx context.Context, sid string) error {
	return e.put(ctx, "/v1/session/renew/"+sid, nil, nil)
}

func (e *ConsulEngine) registerNode(ctx context.Context, node NodeInfo, sid string) error {
	return e.setKey(ctx, consulNodeKey(node), sid, consulValue{Node: node})
}

func (e *ConsulEngine) registerTopic(ctx context.Context, node NodeInfo, topic string, sid string) error {
	return e.setKey(ctx, consulTopicKey(node, topic), sid, consulValue{Node: node})
}

func (e *ConsulEngine) unregisterTopic(ctx context.Context, node NodeInfo, topic string) error {
	return e.unsetKey(ctx, consulTopicKey(node, topic))
}

func (e *ConsulEngine) registerChannel(ctx context.Context, node NodeInfo, topic string, channel string, sid string) error {
	return e.setKey(ctx, consulChannelKey(node, topic, channel), sid, consulValue{Node: node})
}

func (e *ConsulEngine) unregisterChannel(ctx context.Context, node NodeInfo, topic string, channel string) error {
	return e.unsetKey(ctx, consulChannelKey(node, topic, channel))
}

func (e *ConsulEngine) tombstoneTopic(ctx context.Context, node NodeInfo, topic string, sid string, exp time.Time) error {
	return e.setKey(ctx, consulTombstoneKey(node, topic), sid, consulValue{Node: node, Deadline: &exp})
}

func (e *ConsulEngine) listKeys(ctx context.Context, prefix string) (keys []string, err error) {
	if err = e.get(ctx, e.key(prefix)+"?keys", &keys); err != nil {
		return
	}

	cache := make(map[string]bool)
	prefix = e.namespace + "/" + prefix + "/"

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

func (e *ConsulEngine) getNodes(ctx context.Context, key string, now time.Time) (nodes []NodeInfo, err error) {
	var values []consulValue

	if values, err = e.getKey(ctx, key); err != nil {
		if consulErrorNotFound(err) {
			err = nil
		}
		return
	}

	if len(values) != 0 {
		nodes = make([]NodeInfo, 0, len(values))

		for _, v := range values {
			// When a deadline has been set on the key it's filtered out if it
			// has already expired.
			//
			// The reason we need this is to support tombstones with timeouts
			// shorter than the shortest session TTL (consul supports a minimum
			// timeout of 10 seconds).
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

func (e *ConsulEngine) getKey(ctx context.Context, key string) (values []consulValue, err error) {
	var kv []struct{ Value []byte }

	if err = e.get(ctx, e.key(key)+"?recurse", &kv); err != nil {
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

func (e *ConsulEngine) setKey(ctx context.Context, key string, sid string, value consulValue) (err error) {
	return e.put(ctx, e.key(key)+"?acquire="+sid, value, nil)
}

func (e *ConsulEngine) unsetKey(ctx context.Context, key string) error {
	return e.delete(ctx, e.key(key)+"?recurse")
}

func (e *ConsulEngine) key(key string) string {
	return path.Join("/v1/kv", e.namespace, key)
}

func (e *ConsulEngine) get(ctx context.Context, url string, recv interface{}) error {
	return e.do(ctx, "GET", url, nil, recv)
}

func (e *ConsulEngine) put(ctx context.Context, url string, send interface{}, recv interface{}) error {
	return e.do(ctx, "PUT", url, send, recv)
}

func (e *ConsulEngine) delete(ctx context.Context, url string) error {
	return e.do(ctx, "DELETE", url, nil, nil)
}

func (e *ConsulEngine) do(ctx context.Context, method string, url string, send interface{}, recv interface{}) (err error) {
	var req *http.Request
	var res *http.Response
	var b []byte

	if send != nil {
		if b, err = json.Marshal(send); err != nil {
			return
		}

		// Prettify things to make it easier to read in the consul UI or when
		// using curl to read the consul state.
		buf := bytes.Buffer{}
		buf.Grow(3 * len(b))
		if json.Indent(&buf, b, "", "  ") == nil {
			b = buf.Bytes()
		}
	}

	url = e.address + url

	if req, err = http.NewRequest(method, url, bytes.NewReader(b)); err != nil {
		return
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	if res, err = e.client.Do(req); err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		io.Copy(io.Discard, res.Body)
		err = consulError{
			method: method,
			url:    url,
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
		io.Copy(io.Discard, res.Body)
	}

	return
}

type ConsulNode struct {
	eng  *ConsulEngine
	sid  string
	info NodeInfo
}

func newConsulNode(eng *ConsulEngine, sid string, info NodeInfo) *ConsulNode {
	return &ConsulNode{
		eng:  eng,
		sid:  sid,
		info: info,
	}
}

func (n *ConsulNode) String() string {
	return n.info.String() + "/" + n.sid
}

func (n *ConsulNode) Info() NodeInfo {
	return n.info
}

func (n *ConsulNode) Ping(ctx context.Context) error {
	return n.eng.renewSession(ctx, n.sid)
}

func (n *ConsulNode) Unregister(ctx context.Context) error {
	return n.eng.destroySession(ctx, n.sid)
}

func (n *ConsulNode) RegisterTopic(ctx context.Context, topic string) error {
	return n.eng.registerTopic(ctx, n.info, topic, n.sid)
}

func (n *ConsulNode) UnregisterTopic(ctx context.Context, topic string) error {
	return n.eng.unregisterTopic(ctx, n.info, topic)
}

func (n *ConsulNode) RegisterChannel(ctx context.Context, topic string, channel string) error {
	return n.eng.registerChannel(ctx, n.info, topic, channel, n.sid)
}

func (n *ConsulNode) UnregisterChannel(ctx context.Context, topic string, channel string) error {
	return n.eng.unregisterChannel(ctx, n.info, topic, channel)
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

// This error type is used to represent errors from HTTP status code other than
// 200 in responses from the consul agent.
type consulError struct {
	method string
	url    string
	status int
	reason string
}

func (e consulError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.method, e.url, e.reason)
}

// This structure is what the consul engine stores as value in the consul
// key/value store.
type consulValue struct {
	Node     NodeInfo   `json:"nsqd"`
	Deadline *time.Time `json:"deadline,omitempty"`
}
