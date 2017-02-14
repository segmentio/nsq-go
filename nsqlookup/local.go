package nsqlookup

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultLocalEngineNodeTimeout is the maximum amount of time an idle node
	// will be kept by default in a local engine.
	DefaultLocalEngineNodeTimeout = 2 * DefaultReadTimeout

	// DefaultLocalEngineTombstoneTimeout is the maximum amount of time a
	// tombstone rule is kept by default in a local engine.
	DefaultLocalEngineTombstoneTimeout = DefaultLocalEngineNodeTimeout
)

// The LocalConfig structure is used to configure local nsqlookup engines.
type LocalConfig struct {
	// NodeTimeout is the maximum amount of time an idle node will be kept in a
	// local engine.
	NodeTimeout time.Duration

	// TombstoneTimeout is the maximum amount of time a tombstone rule is kept
	// in a local engine.
	TombstoneTimeout time.Duration
}

// LocalEngine is a nsqlookup engine that maintain its state in memory.
//
// This is an implementation of the default behavior of nsqlookup servers as
// provided by the standard implementation, where no state is shared between
// instances of nsqlookup and the state is disarded when the server goes away.
type LocalEngine struct {
	// immutable state
	nodeTimeout time.Duration
	tombTimeout time.Duration

	// graceful shutdown
	done chan struct{}
	join chan struct{}
	once sync.Once

	// mutable state
	mutex sync.RWMutex
	nodes map[string]*LocalNode
}

// NewLocalEngine creates and returns an instance of LocalEngine configured with
// config.
func NewLocalEngine(config LocalConfig) *LocalEngine {
	if config.NodeTimeout == 0 {
		config.NodeTimeout = DefaultLocalEngineNodeTimeout
	}

	if config.TombstoneTimeout == 0 {
		config.TombstoneTimeout = DefaultLocalEngineTombstoneTimeout
	}

	e := &LocalEngine{
		nodeTimeout: config.NodeTimeout,
		tombTimeout: config.TombstoneTimeout,

		done: make(chan struct{}),
		join: make(chan struct{}),

		nodes: make(map[string]*LocalNode),
	}

	go e.run()
	return e
}

func (e *LocalEngine) Close() error {
	e.once.Do(e.close)
	<-e.join
	return nil
}

func (e *LocalEngine) RegisterNode(ctx context.Context, node NodeInfo) (Node, error) {
	now := time.Now()
	exp := now.Add(e.nodeTimeout)
	key := httpBroadcastAddress(node)

	n := newLocalNode(e, node, exp)
	e.mutex.Lock()
	e.nodes[key] = n
	e.mutex.Unlock()

	return n, nil
}

func (e *LocalEngine) unregisterNode(node *LocalNode) error {
	key := httpBroadcastAddress(node.info)
	e.mutex.Lock()
	if e.nodes[key] == node {
		delete(e.nodes, key)
	}
	e.mutex.Unlock()
	return nil
}

func (e *LocalEngine) pingNode(node *LocalNode) (err error) {
	key := httpBroadcastAddress(node.info)
	now := time.Now()
	exp := now.Add(e.nodeTimeout)
	e.mutex.RLock()

	if e.nodes[key] == node {
		node.mutex.Lock()
		if now.After(node.exptime) {
			err = errExpiredNode
		} else {
			node.exptime = exp
		}
		node.mutex.Unlock()
	}

	e.mutex.RUnlock()
	return
}

func (e *LocalEngine) TombstoneTopic(ctx context.Context, node NodeInfo, topic string) error {
	now := time.Now()
	key := httpBroadcastAddress(node)

	e.mutex.RLock()
	n := e.nodes[key]
	e.mutex.RUnlock()

	if n == nil {
		return errMissingNode
	}

	if err := n.checkExpired(now); err != nil {
		return err
	}

	n.tombstoneTopic(topic, now.Add(e.tombTimeout))
	return nil
}

func (e *LocalEngine) LookupNodes(ctx context.Context) (nodes []NodeInfo, err error) {
	e.mutex.RLock()

	for _, node := range e.nodes {
		nodes = append(nodes, node.info)
	}

	e.mutex.RUnlock()
	return
}

func (e *LocalEngine) LookupProducers(ctx context.Context, topic string) (producers []NodeInfo, err error) {
	e.mutex.RLock()

	for _, node := range e.nodes {
		if node.has(topic) {
			producers = append(producers, node.info)
		}
	}

	e.mutex.RUnlock()
	return
}

func (e *LocalEngine) LookupTopics(ctx context.Context) (topics []string, err error) {
	set := make(map[string]bool)
	e.mutex.RLock()

	for _, node := range e.nodes {
		node.lookupTopics(set)
	}

	e.mutex.RUnlock()

	if len(set) != 0 {
		topics = make([]string, 0, len(set))

		for topic := range set {
			topics = append(topics, topic)
		}
	}

	return
}

func (e *LocalEngine) LookupChannels(ctx context.Context, topic string) (channels []string, err error) {
	set := make(map[string]bool)
	e.mutex.RLock()

	for _, node := range e.nodes {
		node.lookupChannels(topic, set)
	}

	e.mutex.RUnlock()

	if len(set) != 0 {
		channels = make([]string, 0, len(set))

		for topic := range set {
			channels = append(channels, topic)
		}
	}

	return
}

func (e *LocalEngine) LookupInfo(ctx context.Context) (info EngineInfo, err error) {
	info.Type = "local"
	info.Version = "0.3.8"
	return
}

func (e *LocalEngine) CheckHealth(ctx context.Context) (err error) {
	return
}

func (e *LocalEngine) run() {
	defer close(e.join)

	t1 := time.NewTicker(e.nodeTimeout / 2)
	defer t1.Stop()

	t2 := time.NewTicker(e.tombTimeout / 2)
	defer t2.Stop()

	for {
		select {
		case <-e.done:
			return

		case now := <-t1.C:
			e.removeExpiredNodes(now)

		case now := <-t2.C:
			e.removeExpiredTombs(now)
		}
	}
}

func (e *LocalEngine) close() {
	close(e.done)
}

func (e *LocalEngine) removeExpiredNodes(now time.Time) {
	e.mutex.Lock()

	for key, node := range e.nodes {
		if now.After(node.exptime) {
			delete(e.nodes, key)
		}
	}

	e.mutex.Unlock()
}

func (e *LocalEngine) removeExpiredTombs(now time.Time) {
	e.mutex.RLock()

	for _, node := range e.nodes {
		node.removeExpiredTombs(now)
	}

	e.mutex.RUnlock()
}

type LocalNode struct {
	// immutable state
	engine *LocalEngine
	info   NodeInfo

	// mutable state
	mutex   sync.RWMutex
	exptime time.Time
	topics  map[string]*localTopic
	tombs   map[string]time.Time
}

func newLocalNode(engine *LocalEngine, info NodeInfo, exp time.Time) *LocalNode {
	return &LocalNode{
		engine:  engine,
		info:    info,
		exptime: exp,
		topics:  make(map[string]*localTopic),
		tombs:   make(map[string]time.Time),
	}
}

func (n *LocalNode) String() string {
	return n.info.String()
}

func (n *LocalNode) Info() NodeInfo {
	return n.info
}

func (n *LocalNode) Ping(ctx context.Context) error {
	return n.engine.pingNode(n)
}

func (n *LocalNode) Unregister(ctx context.Context) error {
	return n.engine.unregisterNode(n)
}

func (n *LocalNode) RegisterTopic(ctx context.Context, topic string) error {
	if err := n.checkExpired(time.Now()); err != nil {
		return err
	}
	n.registerTopic(topic)
	return nil
}

func (n *LocalNode) UnregisterTopic(ctx context.Context, topic string) error {
	if err := n.checkExpired(time.Now()); err != nil {
		return err
	}
	n.unregisterTopic(topic)
	return nil
}

func (n *LocalNode) RegisterChannel(ctx context.Context, topic string, channel string) error {
	if err := n.checkExpired(time.Now()); err != nil {
		return err
	}
	n.registerChannel(topic, channel)
	return nil
}

func (n *LocalNode) UnregisterChannel(ctx context.Context, topic string, channel string) error {
	if err := n.checkExpired(time.Now()); err != nil {
		return err
	}
	n.unregisterChannel(topic, channel)
	return nil
}

func (n *LocalNode) checkExpired(now time.Time) error {
	n.mutex.RLock()
	expired := now.After(n.exptime)
	n.mutex.RUnlock()

	if expired {
		return errExpiredNode
	}

	return nil
}

func (n *LocalNode) registerTopic(topic string) {
	n.mutex.Lock()

	if _, ok := n.topics[topic]; !ok {
		n.topics[topic] = newLocalTopic()
	}

	n.mutex.Unlock()
}

func (n *LocalNode) unregisterTopic(topic string) {
	n.mutex.Lock()
	delete(n.topics, topic)
	n.mutex.Unlock()
}

func (n *LocalNode) tombstoneTopic(topic string, exp time.Time) {
	n.mutex.Lock()
	n.tombs[topic] = exp
	n.mutex.Unlock()
}

func (n *LocalNode) lookupTopics(topics map[string]bool) {
	n.mutex.RLock()

	for topic := range n.topics {
		topics[topic] = true
	}

	n.mutex.RUnlock()
}

func (n *LocalNode) registerChannel(topic string, channel string) {
	n.mutex.RLock()
	t, ok := n.topics[topic]
	n.mutex.RUnlock()

	if !ok {
		n.mutex.Lock()
		t = newLocalTopic()
		n.topics[topic] = t
		n.mutex.Unlock()
	}

	t.registerChannel(channel)
}

func (n *LocalNode) unregisterChannel(topic string, channel string) {
	n.mutex.RLock()

	if t, ok := n.topics[topic]; ok {
		t.unregisterChannel(channel)
	}

	n.mutex.RUnlock()
}

func (n *LocalNode) lookupChannels(topic string, channels map[string]bool) {
	n.mutex.RLock()

	if _, skip := n.tombs[topic]; !skip {
		if t, ok := n.topics[topic]; ok {
			t.lookupChannels(channels)
		}
	}

	n.mutex.RUnlock()
}

func (n *LocalNode) has(topic string) (ok bool) {
	n.mutex.RLock()

	if _, skip := n.tombs[topic]; !skip {
		_, ok = n.topics[topic]
	}

	n.mutex.RUnlock()
	return
}

func (n *LocalNode) removeExpiredTombs(now time.Time) {
	n.mutex.Lock()

	for topic, exptime := range n.tombs {
		if now.After(exptime) {
			delete(n.tombs, topic)
		}
	}

	n.mutex.Unlock()
}

type localTopic struct {
	mutex    sync.RWMutex
	channels map[string]bool
}

func newLocalTopic() *localTopic {
	return &localTopic{
		channels: make(map[string]bool),
	}
}

func (t *localTopic) registerChannel(channel string) {
	t.mutex.Lock()
	t.channels[channel] = true
	t.mutex.Unlock()
}

func (t *localTopic) unregisterChannel(channel string) {
	t.mutex.Lock()
	delete(t.channels, channel)
	t.mutex.Unlock()
}

func (t *localTopic) lookupChannels(channels map[string]bool) {
	t.mutex.RLock()

	for channel := range t.channels {
		channels[channel] = true
	}

	t.mutex.RUnlock()
}
