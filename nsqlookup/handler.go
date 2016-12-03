package nsqlookup

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"
)

// The DiscoverHandler type provides the implementation of a connection handler
// that speaks the nsqlookupd discovery protocol and provides an interface to a
// nsqlookup engine.
type NodeHandler struct {
	// Engine must not be nil and has to be set to the engine that will be used
	// by the handler to register the connections it serves.
	Engine Engine

	// The Info field should be set to provide information to the connections
	// about the discovery endpoint they're connected to.
	Info NodeInfo

	// ReadTimeout is the maximum amount of time the handler will allow its
	// connections to be idle before closing them.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum amount of time the handler will take to send
	// responses to its connections.
	WriteTimeout time.Duration
}

// ServeConn takes ownership of the conn object and starts service the commands
// that the client sends to the discovery handler.
func (n *NodeHandler) ServeConn(conn net.Conn) (err error) {
	const bufSize = 2048

	var node NodeInfo
	var r = bufio.NewReaderSize(conn, bufSize)
	var w = bufio.NewWriterSize(conn, bufSize)

	defer conn.Close()
	defer func() {
		if node != (NodeInfo{}) {
			n.Engine.UnregisterNode(node)
		}
	}()

	host, port, _ := net.SplitHostPort(conn.LocalAddr().String())

	if n.Info.TcpPort == 0 {
		n.Info.TcpPort, _ = strconv.Atoi(port)
	}

	if len(n.Info.BroadcastAddress) == 0 {
		n.Info.BroadcastAddress = host
	}

	if len(n.Info.Hostname) == 0 {
		n.Info.Hostname, _ = os.Hostname()
	}

	if len(n.Info.Version) == 0 {
		info, _ := n.Engine.LookupInfo()
		n.Info.Version = info.Version
	}

	for {
		var cmd Command
		var res Response

		if cmd, err = n.readCommand(conn, r); err == nil {
			switch c := cmd.(type) {
			case Ping:
				res, err = n.ping(node)

			case Identify:
				node, res, err = n.identify(node, c.Info)

			case Register:
				res, err = n.register(node, c.Topic, c.Channel)

			case Unregister:
				res, err = n.unregister(node, c.Topic, c.Channel)

			default:
				res = Error{Code: ErrInvalid, Reason: "unknown command"}
			}
		}

		if err != nil {
			switch e := err.(type) {
			case Error:
				res = e

			case net.Error:
				if e.Timeout() {
					return // no ping received for longer than the ping timeout
				}
				if e.Temporary() {
					time.Sleep(100 * time.Millisecond)
					continue // try again after waiting for a little while
				}
				return

			default:
				res = Error{Code: ErrInvalid, Reason: err.Error()}
			}
		}

		if err = n.writeResponse(conn, w, res); err != nil {
			return
		}
	}
}

func (n *NodeHandler) identify(node NodeInfo, info NodeInfo) (id NodeInfo, res RawResponse, err error) {
	if node != (NodeInfo{}) {
		id, err = node, errCannotIdentifyAgain
		return
	}
	b, _ := json.Marshal(n.Info)
	id, res = info, RawResponse(b)
	return
}

func (n *NodeHandler) ping(node NodeInfo) (res OK, err error) {
	if node != (NodeInfo{}) { // ping may arrive before identify
		err = n.Engine.PingNode(node)
	}
	return
}

func (n *NodeHandler) register(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = n.Engine.RegisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = n.Engine.RegisterTopic(node, topic)

	default:
		err = n.Engine.RegisterNode(node)
	}

	return
}

func (n *NodeHandler) unregister(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = n.Engine.UnregisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = n.Engine.UnregisterTopic(node, topic)

	default:
		err = n.Engine.UnregisterNode(node)
	}

	return
}

func (n *NodeHandler) readCommand(c net.Conn, r *bufio.Reader) (cmd Command, err error) {
	if err = c.SetReadDeadline(time.Now().Add(n.ReadTimeout)); err == nil {
		cmd, err = ReadCommand(r)
	}
	return
}

func (n *NodeHandler) writeResponse(c net.Conn, w *bufio.Writer, r Response) (err error) {
	if err = c.SetWriteDeadline(time.Now().Add(n.WriteTimeout)); err == nil {
		if err = r.Write(w); err == nil {
			err = w.Flush()
		}
	}
	return
}
