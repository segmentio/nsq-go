package nsqlookup

import "testing"

func TestIdentify(t *testing.T) {
	testCommand(t, "IDENTIFY", Identify{
		Info: PeerInfo{
			RemoteAddress:    "127.0.0.1:12345",
			Hostname:         "localhost",
			BroadcastAddress: "127.0.0.1:4150",
			TcpPort:          4150,
			HttpPort:         4151,
			Version:          "1.0.0",
		},
	})
}
