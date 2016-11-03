package nsqlookup

type PeerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TcpPort          int    `json:"tcp_port"`
	HttpPort         int    `json:"http_port"`
	Version          string `json:"version"`
}
