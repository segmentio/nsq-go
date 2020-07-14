package nsqlookup

import (
	"context"
	"net"
	"sort"
	"strconv"
)

// The NodeInfo structure carries information about a node referenced by a
// nsqlookup server.
type NodeInfo struct {
	// RemoteAddress is the address that the node connected from.
	RemoteAddress string `json:"remote_address"`

	// Hostname of the nsqd node.
	Hostname string `json:"hostname"`

	// BroadcastAddress is the address advertized by the nsqd node.
	BroadcastAddress string `json:"broadcast_address"`

	// TcpPort is the port on which the nsqd node is listening for incoming TCP
	// connections.
	TcpPort int `json:"tcp_port"`

	// HttpPort is the port on which the nsqd node accepts HTTP requests.
	HttpPort int `json:"http_port"`

	// Version represents the version of nsqd ran by the node.
	Version string `json:"version"`
}

// String returns a human-readable representation of the node info.
func (info NodeInfo) String() string {
	if len(info.Hostname) != 0 {
		return info.Hostname
	}
	return httpBroadcastAddress(info)
}

// The NodeInfo2 structure carries information about a node referenced by a
// nsqlookup server.
//
// The type is very similar to NodeInfo, but adds a list of tombstones for a
// node, and a list of topics. The tombstones list carries booleans that tell
// whether the topic at the matching index has been tombstoned on the node.
type NodeInfo2 struct {
	// RemoteAddress is the address that the node connected from.
	RemoteAddress string `json:"remote_address"`

	// Hostname of the nsqd node.
	Hostname string `json:"hostname"`

	// BroadcastAddress is the address advertized by the nsqd node.
	BroadcastAddress string `json:"broadcast_address"`

	// TcpPort is the port on which the nsqd node is listening for incoming TCP
	// connections.
	TcpPort int `json:"tcp_port"`

	// HttpPort is the port on which the nsqd node accepts HTTP requests.
	HttpPort int `json:"http_port"`

	// Version represents the version of nsqd ran by the node.
	Version string `json:"version"`

	// Tombstones has items set to true if the topic at the matching index has
	// been tomstoned.
	Tombstones []bool `json:"tombstones"`

	// Topics is the list of topic hosted by the node.
	Topics []string `json:"topics"`
}

// String returns a human-readable representation of the node info.
func (info NodeInfo2) String() string {
	if len(info.Hostname) != 0 {
		return info.Hostname
	}
	return makeBroadcastAddress(info.BroadcastAddress, info.HttpPort)
}

// The EngineInfo structure carries information about a nsqlookup engine.
type EngineInfo struct {
	// Type of the engine.
	Type string `json:"type"`

	// Version represents the version of the nsqlookup engine.
	Version string `json:"version"`
}

// The Engine interface must be implemented by types that are intended to be
// used to power nsqlookup servers.
//
// Each method of the engine accepts a context as first argument which may be
// used to cancel or set a deadline on the operation.
// This is useful for engines that work we storage services accessed over the
// network.
// The context may be nil.
type Engine interface {
	// Close should release all internal state maintained by the engine, it is
	// called when the nsqlookup server using the engine is shutting down.
	Close() error

	// RegisterNode is called by nsqlookup servers when a new node is attempting
	// to register.
	RegisterNode(ctx context.Context, node NodeInfo) (Node, error)

	// TombstoneTopic marks topic as tombstoned on node.
	TombstoneTopic(ctx context.Context, node NodeInfo, topic string) error

	// LookupNodes must return a list of of all nodes registered on the engine.
	LookupNodes(ctx context.Context) ([]NodeInfo2, error)

	// LookupProducers must return a list of all nodes for which topic has been
	// registered on the engine and were not tombstoned.
	LookupProducers(ctx context.Context, topic string) ([]NodeInfo, error)

	// LookupTopics must return a list of all topics registered on the engine.
	LookupTopics(ctx context.Context) ([]string, error)

	// LookupChannels must return a list of all channels registered for topic on
	// the engine.
	LookupChannels(ctx context.Context, topic string) ([]string, error)

	// LookupInfo must return information about the engine.
	LookupInfo(ctx context.Context) (EngineInfo, error)

	// CheckHealth is called by nsqlookup servers to evaluate the health of the
	// engine.
	CheckHealth(ctx context.Context) error
}

// The Node interface is used to represent a single node registered within a
// nsqlookup engine.
type Node interface {
	// Info should return the info given to RegisterNode when the node was
	// created.
	Info() NodeInfo

	// Ping is called by nsqlookup servers when a registered node sends a
	// ping command to inform that it is still alive.
	Ping(ctx context.Context) error

	// Unregister is called by nsqlookup servers when a node that had
	// previously registered is going away.
	Unregister(ctx context.Context) error

	// RegisterTopic is called by nsqlookup servers when topic is being
	// registered on node.
	RegisterTopic(ctx context.Context, topic string) error

	// UnregisterTopic is called by nsqlookup servers when topic is being
	// unregistered from node.
	UnregisterTopic(ctx context.Context, topic string) error

	// RegisterChannel is called by nsqlookup servers when channel from topic is
	// being registered on node.
	RegisterChannel(ctx context.Context, topic string, channel string) error

	// UnregisterChannel is called by nsqlookup servers when channel from topic
	// is being unregistered from node.
	UnregisterChannel(ctx context.Context, topic string, channel string) error
}

type byNode []NodeInfo

func (n byNode) Len() int {
	return len(n)
}

func (n byNode) Swap(i int, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n byNode) Less(i int, j int) bool {
	n1 := &n[i]
	n2 := &n[j]
	return (n1.BroadcastAddress < n2.BroadcastAddress) ||
		(n1.BroadcastAddress == n2.BroadcastAddress && n1.TcpPort < n2.TcpPort)
}

type byNode2 []NodeInfo2

func (n byNode2) Len() int {
	return len(n)
}

func (n byNode2) Swap(i int, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n byNode2) Less(i int, j int) bool {
	n1 := &n[i]
	n2 := &n[j]
	return (n1.BroadcastAddress < n2.BroadcastAddress) ||
		(n1.BroadcastAddress == n2.BroadcastAddress && n1.TcpPort < n2.TcpPort)
}

func nonNilNodes(n []NodeInfo) []NodeInfo {
	if n == nil {
		n = []NodeInfo{}
	}
	return n
}

func nonNilNodes2(n []NodeInfo2) []NodeInfo2 {
	if n == nil {
		n = []NodeInfo2{}
	}
	return n
}

func nonNilStrings(s []string) []string {
	if s == nil {
		s = []string{}
	}
	return s
}

func sortedNodes(n []NodeInfo) []NodeInfo {
	sort.Sort(byNode(n))
	return n
}

func sortedNodes2(n []NodeInfo2) []NodeInfo2 {
	sort.Sort(byNode2(n))
	return n
}

func httpBroadcastAddress(info NodeInfo) string {
	return makeBroadcastAddress(info.BroadcastAddress, info.HttpPort)
}

func tcpBroadcastAddress(info NodeInfo) string {
	return makeBroadcastAddress(info.BroadcastAddress, info.TcpPort)
}

func makeBroadcastAddress(addr string, port int) string {
	host, _, _ := net.SplitHostPort(addr)
	if len(host) == 0 {
		host = addr // no port in addr
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}
