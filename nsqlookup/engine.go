package nsqlookup

import (
	"net"
	"strconv"
)

type NodeInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type EngineInfo struct {
	Version string `json:"version"`
}

type Engine interface {
	RegisterNode(node NodeInfo) error

	UnregisterNode(node NodeInfo) error

	TombstoneNode(node NodeInfo, topic string) error

	RegisterTopic(node NodeInfo, topic string) error

	UnregisterTopic(node NodeInfo, topic string) error

	RegisterChannel(node NodeInfo, topic string, channel string) error

	UnregisterChannel(node NodeInfo, topic string, channel string) error

	LookupNodes() ([]NodeInfo, error)

	LookupProducers(topic string) ([]NodeInfo, error)

	LookupTopics() ([]string, error)

	LookupChannels(topic string) ([]string, error)

	LookupInfo() (EngineInfo, error)

	CheckHealth() error
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
