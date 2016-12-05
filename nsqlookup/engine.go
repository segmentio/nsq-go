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
// Each method of the engine accepts a context as last argument which may be
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
	RegisterNode(node NodeInfo, ctx context.Context) error

	// UnregisterNode is called by nsqlookup servers when a node that had
	// previously registered is going away.
	UnregisterNode(node NodeInfo, ctx context.Context) error

	// PingNode is called by nsqlookup servers when a registered node sends a
	// ping command to inform that it is still alive.
	PingNode(node NodeInfo, ctx context.Context) error

	// TombstoneTopic marks topic as tombstoned on node.
	TombstoneTopic(node NodeInfo, topic string, ctx context.Context) error

	// RegisterTopic is called by nsqlookup servers when topic is being
	// registered on node.
	RegisterTopic(node NodeInfo, topic string, ctx context.Context) error

	// UnregisterTopic is called by nsqlookup servers when topic is being
	// unregistered from node.
	UnregisterTopic(node NodeInfo, topic string, ctx context.Context) error

	// RegisterChannel is called by nsqlookup servers when channel from topic is
	// being registered on node.
	RegisterChannel(node NodeInfo, topic string, channel string, ctx context.Context) error

	// UnregisterChannel is called by nsqlookup servers when channel from topic
	// is being unregistered from node.
	UnregisterChannel(node NodeInfo, topic string, channel string, ctx context.Context) error

	// LookupNodes must return a list of of all nodes registered on the engine.
	LookupNodes(ctx context.Context) ([]NodeInfo, error)

	// LookupProducers must return a list of all nodes for which topic has been
	// registered on the engine and were not tombstoned.
	LookupProducers(topic string, ctx context.Context) ([]NodeInfo, error)

	// LookupTopics must return a list of all topics registered on the engine.
	LookupTopics(ctx context.Context) ([]string, error)

	// LookupChannels must return a list of all channels registerd for topic on
	// the engine.
	LookupChannels(topic string, ctx context.Context) ([]string, error)

	// LookupInfo must return information about the engine.
	LookupInfo(ctx context.Context) (EngineInfo, error)

	// CheckHealth is called by nsqlookup servers to evaluate the health of the
	// engine.
	CheckHealth(ctx context.Context) error
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

func sortedNodes(n []NodeInfo) []NodeInfo {
	sort.Sort(byNode(n))
	return n
}

func sortedStrings(s []string) []string {
	sort.Strings(s)
	return s
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
