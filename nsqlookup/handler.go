package nsqlookup

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
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
	DefaultWriteTimeout = 1 * time.Second

	// DefaultEngineTimeout is the maximum duration used by default for engine
	// operations.
	DefaultEngineTimeout = 1 * time.Second
)

// The HTTPHandler satisfies the http.Handler interface and provides the
// implementation of the nsqlookup http API.
type HTTPHandler struct {
	// Engine must not be nil and has to be set to the engine that will be used
	// by the handler to respond to http requests.
	Engine Engine

	// EngineTimeout should be set to the maximum duration allowed for engine
	// operations.
	EngineTimeout time.Duration
}

func (h HTTPHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	if h.EngineTimeout == 0 {
		h.EngineTimeout = DefaultEngineTimeout
	}

	ctx, _ := context.WithTimeout(req.Context(), h.EngineTimeout)
	req = req.WithContext(ctx)

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

func (h HTTPHandler) serveLookup(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	nodes, err := h.Engine.LookupProducers(req.Context(), topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{nonNilNodes(sortedNodes(nodes))})
}

func (h HTTPHandler) serveTopics(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	topics, err := h.Engine.LookupTopics(req.Context())
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", struct {
		Topics []string `json:"topics"`
	}{nonNilStrings(sortedStrings(topics))})
}

func (h HTTPHandler) serveChannels(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	channels, err := h.Engine.LookupChannels(req.Context(), topic)
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", struct {
		Channels []string `json:"channels"`
	}{nonNilStrings(sortedStrings(channels))})
}

func (h HTTPHandler) serveNodes(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	nodes, err := h.Engine.LookupNodes(req.Context())
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{nonNilNodes(sortedNodes(nodes))})
}

func (h HTTPHandler) servePing(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	if err := h.Engine.CheckHealth(req.Context()); err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendOK(res)
}

func (h HTTPHandler) serveInfo(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	info, err := h.Engine.LookupInfo(req.Context())
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", info)
}

func (h HTTPHandler) serveDeleteTopic(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_topic") {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	info, err := h.Engine.LookupInfo(req.Context())
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	nodes, err := h.Engine.LookupProducers(req.Context(), topic)
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

	h.sendResponse(res, req, 200, "OK", nil)
}

func (h HTTPHandler) serveDeleteChannel(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_channel") {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	channel := query.Get("channel")

	if len(topic) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(channel) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_CHANNEL", nil)
		return
	}

	info, err := h.Engine.LookupInfo(req.Context())
	if err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	nodes, err := h.Engine.LookupProducers(req.Context(), topic)
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

	h.sendResponse(res, req, 200, "OK", nil)
}

func (h HTTPHandler) serveTombstoneTopicProducer(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && req.Method != "GET" {
		h.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	node := query.Get("node")

	if len(topic) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(node) == 0 {
		h.sendResponse(res, req, 500, "MISSING_ARG_NODE", nil)
		return
	}

	host, port, err := net.SplitHostPort(node)
	if err != nil {
		h.sendResponse(res, req, 500, "BAD_ARG_NODE", nil)
		return
	}

	intport, err := strconv.Atoi(port)
	if err != nil {
		h.sendResponse(res, req, 500, "BAD_ARG_NODE", nil)
		return
	}

	if err := h.Engine.TombstoneTopic(req.Context(), NodeInfo{
		BroadcastAddress: host,
		HttpPort:         intport,
	}, topic); err != nil {
		h.sendInternalServerError(res, err)
		return
	}

	h.sendResponse(res, req, 200, "OK", nil)
}

func (h HTTPHandler) sendResponse(res http.ResponseWriter, req *http.Request, status int, text string, value interface{}) {
	v1 := req.Header.Get("Accept") == "application/vnd.nsq; version=1.0"

	if !v1 {
		value = struct {
			StatusCode int         `json:"status_code"`
			StatusText string      `json:"status_txt"`
			Data       interface{} `json:"data"`
		}{
			StatusCode: status,
			StatusText: text,
			Data:       value,
		}
	}

	hdr := res.Header()
	hdr.Set("Content-Type", "application/json; charset=utf-8")

	if v1 {
		hdr.Set("X-NSQ-Content-Type", "nsq; version=1.0")
	}

	res.WriteHeader(status)
	json.NewEncoder(res).Encode(value)
}

func (h HTTPHandler) sendOK(res http.ResponseWriter) {
	res.WriteHeader(http.StatusOK)
	res.Write([]byte("OK"))
}

func (h HTTPHandler) sendNotFound(res http.ResponseWriter) {
	h.sendError(res, 404, "NOT_FOUND")
}

func (h HTTPHandler) sendMethodNotAllowed(res http.ResponseWriter) {
	h.sendError(res, 405, "METHOD_NOT_ALLOWED")
}

func (h HTTPHandler) sendInternalServerError(res http.ResponseWriter, err error) {
	h.sendError(res, 500, err.Error())
}

func (h HTTPHandler) sendError(res http.ResponseWriter, status int, message string) {
	res.WriteHeader(status)
	json.NewEncoder(res).Encode(struct {
		Message string `json:"message"`
	}{message})
}

// The DiscoverHandler type provides the implementation of a connection handler
// that speaks the nsqlookupd discovery protocol and provides an interface to a
// nsqlookup engine.
type TCPHandler struct {
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

	// EngineTimeout is the maximum amount of time the handler gives to
	// operations done on the engine.
	EngineTimeout time.Duration
}

// ServeConn takes ownership of the conn object and starts service the commands
// that the client sends to the discovery handler.
func (h TCPHandler) ServeConn(ctx context.Context, conn net.Conn) {
	const bufSize = 2048

	var node NodeInfo
	var r = bufio.NewReaderSize(conn, bufSize)
	var w = bufio.NewWriterSize(conn, bufSize)

	if h.ReadTimeout == 0 {
		h.ReadTimeout = DefaultReadTimeout
	}

	if h.WriteTimeout == 0 {
		h.WriteTimeout = DefaultWriteTimeout
	}

	if h.EngineTimeout == 0 {
		h.EngineTimeout = DefaultEngineTimeout
	}

	if ctx == nil {
		ctx = context.Background()
	}

	engineContext := func(ctx context.Context) context.Context {
		ectx, _ := context.WithTimeout(ctx, h.EngineTimeout)
		return ectx
	}

	defer func() {
		if node != (NodeInfo{}) {
			h.Engine.UnregisterNode(engineContext(ctx), node)
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
		info, _ := h.Engine.LookupInfo(engineContext(ctx))
		h.Info.Version = info.Version
	}

	var cmdChan = make(chan Command)
	var resChan = make(chan Response)
	var errChan = make(chan error, 2)
	var doneChan = ctx.Done()

	defer close(resChan)

	go h.readLoop(ctx, conn, r, cmdChan, errChan)
	go h.writeLoop(ctx, conn, w, resChan, errChan)

	for {
		var cmd Command
		var res Response
		var err error

		select {
		case <-doneChan:
			return
		case <-errChan:
			return
		case cmd = <-cmdChan:
		}

		switch c := cmd.(type) {
		case Ping:
			res, err = h.ping(engineContext(ctx), node)

		case Identify:
			node, res, err = h.identify(engineContext(ctx), node, c.Info, conn)

		case Register:
			res, err = h.register(engineContext(ctx), node, c.Topic, c.Channel)

		case Unregister:
			node, res, err = h.unregister(engineContext(ctx), node, c.Topic, c.Channel)

		default:
			res = makeErrInvalid("unknown command")
		}

		if err != nil {
			switch e := err.(type) {
			case Error:
				res = e
			default:
				log.Print(err)
				return
			}
		}

		select {
		case <-doneChan:
			return
		case <-errChan:
			return
		case resChan <- res:
		}
	}
}

func (h TCPHandler) identify(ctx context.Context, node NodeInfo, info NodeInfo, conn net.Conn) (id NodeInfo, res RawResponse, err error) {
	if node != (NodeInfo{}) {
		id, err = node, errCannotIdentifyAgain
		return
	}

	if len(info.RemoteAddress) == 0 {
		info.RemoteAddress = conn.RemoteAddr().String()
	}

	b, _ := json.Marshal(h.Info)
	id, res = info, RawResponse(b)
	err = h.Engine.RegisterNode(ctx, info)

	log.Printf("IDENTIFY node = %v, err = %v", info, err)
	return
}

func (h TCPHandler) ping(ctx context.Context, node NodeInfo) (res OK, err error) {
	if node != (NodeInfo{}) { // ping may arrive before identify
		err = h.Engine.PingNode(ctx, node)
		log.Printf("PING node = %v, err = %v", node, err)
	}
	return
}

func (h TCPHandler) register(ctx context.Context, node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = h.Engine.RegisterChannel(ctx, node, topic, channel)

	case len(topic) != 0:
		err = h.Engine.RegisterTopic(ctx, node, topic)

	default:
		err = makeErrBadTopic("missing topic name")
	}

	log.Printf("REGISTER node = %v, err = %v", node, err)
	return
}

func (h TCPHandler) unregister(ctx context.Context, node NodeInfo, topic string, channel string) (id NodeInfo, res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = h.Engine.UnregisterChannel(ctx, node, topic, channel)

	case len(topic) != 0:
		err = h.Engine.UnregisterTopic(ctx, node, topic)

	default:
		err = makeErrBadTopic("missing topic name")
	}

	if err != nil {
		id = node
	}

	log.Printf("UNREGISTER node = %v, err = %v", node, err)
	return
}

func (h TCPHandler) readLoop(ctx context.Context, conn net.Conn, r *bufio.Reader, cmdChan chan<- Command, errChan chan<- error) {
	version, err := h.readVersion(conn, r)

	if err != nil {
		errChan <- err
		return
	}

	if version != "  V1" {
		errChan <- makeErrBadProtocol("unsupported version: %#v", version)
		return
	}

	for {
		for attempt := 0; true; attempt++ {
			var cmd Command
			var err error

			if cmd, err = h.readCommand(conn, r); err == nil {
				cmdChan <- cmd
				break
			}

			if attempt < 10 && isTemporary(err) && !isTimeout(err) {
				sleep(ctx, backoff(attempt, 1*time.Second))
				continue
			}

			errChan <- err
			return
		}
	}
}

func (h TCPHandler) readVersion(c net.Conn, r *bufio.Reader) (version string, err error) {
	if err = c.SetReadDeadline(time.Now().Add(h.ReadTimeout)); err == nil {
		var b [4]byte

		if _, err = io.ReadFull(r, b[:]); err != nil {
			return
		}

		version = string(b[:])
	}
	return
}

func (h TCPHandler) readCommand(c net.Conn, r *bufio.Reader) (cmd Command, err error) {
	if err = c.SetReadDeadline(time.Now().Add(h.ReadTimeout)); err == nil {
		cmd, err = ReadCommand(r)
	}
	return
}

func (h TCPHandler) writeLoop(ctx context.Context, conn net.Conn, w *bufio.Writer, resChan <-chan Response, errChan chan<- error) {
	for res := range resChan {
		for attempt := 0; true; attempt++ {
			err := h.writeResponse(conn, w, res)

			if err == nil {
				break
			}

			if attempt < 10 && isTemporary(err) && !isTimeout(err) {
				sleep(ctx, backoff(attempt, 1*time.Second))
				attempt++
				continue
			}

			errChan <- err
			return
		}
	}
}

func (h TCPHandler) writeResponse(c net.Conn, w *bufio.Writer, r Response) (err error) {
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
