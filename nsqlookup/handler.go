package nsqlookup

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	nsq "github.com/segmentio/nsq-go"
)

const (
	// DefaultTcpAddress is the default address used for TCP connections.
	DefaultTcpAddress = "localhost:4160"

	// DefaultHttpAddress is the default address used for HTTP requests.
	DefaultHttpAddress = "localhost:4161"

	// DefaultReadTimeout is the maximum duration used by default waiting
	// for commands.
	DefaultReadTimeout = 1 * time.Minute

	// DefaultReadTimeout is the maximum duration used by default for write
	// operations.
	DefaultWriteTimeout = 10 * time.Second
)

// The APIHandler satisfies the http.Handler interface and provides the
// implementation of the nsqlookup http API.
type APIHandler struct {
	// Engine must not be nil and has to be set to the engine that will be used
	// by the handler to respond to http requests.
	Engine Engine
}

func (h APIHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/lookup":
		h.serveLookup(res, req)

	case "/topics":
		h.serveTopics(res, req)

	case "/channels":
		h.serveChannels(res, req)

	case "/nodes":
		h.serveNodes(res, req)

	case "/ping":
		h.servePing(res, req)

	case "/info":
		h.serveInfo(res, req)

	case "/topic/delete", "/delete_topic":
		h.serveDeleteTopic(res, req)

	case "/channel/delete", "/delete_channel":
		h.serveDeleteChannel(res, req)

	case "/tombstone_topic_producer":
		h.serveTombstoneTopicProducer(res, req)

	default:
		h.sendNotFound(res)
	}
}

func (h APIHandler) serveLookup(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	nodes, err := h.Engine.LookupProducers(topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{sortedNodes(nodes)})
}

func (h APIHandler) serveTopics(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	topics, err := h.Engine.LookupTopics()
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", struct {
		Topics []string `json:"topics"`
	}{sortedStrings(topics)})
}

func (h APIHandler) serveChannels(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	channels, err := h.Engine.LookupChannels(topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", struct {
		Channels []string `json:"channels"`
	}{sortedStrings(channels)})
}

func (h APIHandler) serveNodes(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	nodes, err := h.Engine.LookupNodes()
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{sortedNodes(nodes)})
}

func (h APIHandler) servePing(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	if err := h.Engine.CheckHealth(); err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendOK(res)
}

func (h APIHandler) serveInfo(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	info, err := h.Engine.LookupInfo()
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", info)
}

func (h APIHandler) serveDeleteTopic(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_topic") {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	info, err := h.Engine.LookupInfo()
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	nodes, err := h.Engine.LookupProducers(topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	errChan := make(chan error, len(nodes))
	userAgent := "nsqlookupd/" + info.Version

	for _, node := range nodes {
		go func(client nsq.Client) { errChan <- client.DeleteTopic(topic) }(nsq.Client{
			Client:    http.Client{Timeout: 10 * time.Second},
			Address:   httpBroadcastAddress(node),
			UserAgent: userAgent,
		})
	}

	for i := 0; i != len(nodes); i++ {
		if e := <-errChan; e != nil {
			err = e
		}
	}

	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", nil)
}

func (h APIHandler) serveDeleteChannel(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_channel") {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	channel := query.Get("channel")

	if len(topic) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(channel) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_CHANNEL", nil)
		return
	}

	info, err := h.Engine.LookupInfo()
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	nodes, err := h.Engine.LookupProducers(topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	errChan := make(chan error, len(nodes))
	userAgent := "nsqlookupd/" + info.Version

	for _, node := range nodes {
		go func(client nsq.Client) { errChan <- client.DeleteChannel(topic, channel) }(nsq.Client{
			Client:    http.Client{Timeout: 10 * time.Second},
			Address:   httpBroadcastAddress(node),
			UserAgent: userAgent,
		})
	}

	for i := 0; i != len(nodes); i++ {
		if e := <-errChan; e != nil {
			err = e
		}
	}

	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", nil)
}

func (h APIHandler) serveTombstoneTopicProducer(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	node := query.Get("node")

	if len(topic) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(node) == 0 {
		h.sendResponse(res, 500, "MISSING_ARG_NODE", nil)
		return
	}

	host, port, err := net.SplitHostPort(node)
	if err != nil {
		h.sendResponse(res, 500, "BAD_ARG_NODE", nil)
		return
	}

	intport, err := strconv.Atoi(port)
	if err != nil {
		h.sendResponse(res, 500, "BAD_ARG_NODE", nil)
		return
	}

	if err := h.Engine.TombstoneNode(NodeInfo{
		BroadcastAddress: host,
		HttpPort:         intport,
	}, topic); err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, 200, "OK", nil)
}

func (h APIHandler) sendResponse(res http.ResponseWriter, status int, text string, value interface{}) {
	res.WriteHeader(status)
	json.NewEncoder(res).Encode(struct {
		StatusCode int         `json:"status_code"`
		StatusText string      `json:"status_txt"`
		Data       interface{} `json:"data"`
	}{
		StatusCode: status,
		StatusText: text,
		Data:       value,
	})
}

func (h APIHandler) sendOK(res http.ResponseWriter) {
	res.WriteHeader(http.StatusOK)
	res.Write([]byte("OK"))
}

func (h APIHandler) sendNotFound(res http.ResponseWriter) {
	h.sendError(res, 404, "NOT_FOUND")
}

func (h APIHandler) sendMethodNotAllowed(res http.ResponseWriter) {
	h.sendError(res, 405, "METHOD_NOT_ALLOWED")
}

func (h APIHandler) sendInternalServerError(res http.ResponseWriter, err error) {
	h.sendError(res, 500, err.Error())
}

func (h APIHandler) sendError(res http.ResponseWriter, status int, message string) {
	res.WriteHeader(status)
	json.NewEncoder(res).Encode(struct {
		Message string `json:"message"`
	}{message})
}

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
func (h NodeHandler) ServeConn(conn net.Conn) (err error) {
	const bufSize = 2048

	var node NodeInfo
	var r = bufio.NewReaderSize(conn, bufSize)
	var w = bufio.NewWriterSize(conn, bufSize)

	defer conn.Close()
	defer func() {
		if node != (NodeInfo{}) {
			h.Engine.UnregisterNode(node)
		}
	}()

	host, port, _ := net.SplitHostPort(conn.LocalAddr().String())

	if h.Info.TcpPort == 0 {
		h.Info.TcpPort, _ = strconv.Atoi(port)
	}

	if len(h.Info.BroadcastAddress) == 0 {
		h.Info.BroadcastAddress = host
	}

	if len(h.Info.Hostname) == 0 {
		h.Info.Hostname, _ = os.Hostname()
	}

	if len(h.Info.Version) == 0 {
		info, _ := h.Engine.LookupInfo()
		h.Info.Version = info.Version
	}

	if h.ReadTimeout == 0 {
		h.ReadTimeout = DefaultReadTimeout
	}

	if h.WriteTimeout == 0 {
		h.WriteTimeout = DefaultWriteTimeout
	}

	for {
		var cmd Command
		var res Response

		if cmd, err = h.readCommand(conn, r); err == nil {
			switch c := cmd.(type) {
			case Ping:
				res, err = h.ping(node)

			case Identify:
				node, res, err = h.identify(node, c.Info)

			case Register:
				res, err = h.register(node, c.Topic, c.Channel)

			case Unregister:
				res, err = h.unregister(node, c.Topic, c.Channel)

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

		if err = h.writeResponse(conn, w, res); err != nil {
			return
		}
	}
}

func (h NodeHandler) identify(node NodeInfo, info NodeInfo) (id NodeInfo, res RawResponse, err error) {
	if node != (NodeInfo{}) {
		id, err = node, errCannotIdentifyAgain
		return
	}
	b, _ := json.Marshal(h.Info)
	id, res = info, RawResponse(b)
	return
}

func (h NodeHandler) ping(node NodeInfo) (res OK, err error) {
	if node != (NodeInfo{}) { // ping may arrive before identify
		err = h.Engine.PingNode(node)
	}
	return
}

func (h NodeHandler) register(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = h.Engine.RegisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = h.Engine.RegisterTopic(node, topic)

	default:
		err = h.Engine.RegisterNode(node)
	}

	return
}

func (h NodeHandler) unregister(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = h.Engine.UnregisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = h.Engine.UnregisterTopic(node, topic)

	default:
		err = h.Engine.UnregisterNode(node)
	}

	return
}

func (h NodeHandler) readCommand(c net.Conn, r *bufio.Reader) (cmd Command, err error) {
	if err = c.SetReadDeadline(time.Now().Add(h.ReadTimeout)); err == nil {
		cmd, err = ReadCommand(r)
	}
	return
}

func (h NodeHandler) writeResponse(c net.Conn, w *bufio.Writer, r Response) (err error) {
	if err = c.SetWriteDeadline(time.Now().Add(h.WriteTimeout)); err == nil {
		if err = r.Write(w); err == nil {
			err = w.Flush()
		}
	}
	return
}

var (
	errClientMustIdentify  = errors.New("client must identify")
	errCannotIdentifyAgain = errors.New("cannot identify again")
	errMissingNode         = errors.New("the node doesn't exist")
)
