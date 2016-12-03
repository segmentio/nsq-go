package nsqlookup

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	nsq "github.com/segmentio/nsq-go"
)

const (
	// DefaultTcpAddress is the default address used for TCP connections.
	DefaultTcpAddress = "localhost:4160"

	// DefaultHttpAddress is the default address used for HTTP requests.
	DefaultHttpAddress = "localhost:4161"

	// DefaultPingTimeout is the maximum duration used by default waiting
	// for ping commands.
	DefaultPingTimeout = 1 * time.Minute

	// DefaultReadTimeout is the maximum duration used by default for read
	// operations.
	DefaultReadTimeout = 5 * time.Second

	// DefaultReadTimeout is the maximum duration used by default for write
	// operations.
	DefaultWriteTimeout = 5 * time.Second
)

var (
	// ErrMissingEngine is returned by StartServer when the Engine field of the
	// configuration is set to nil.
	ErrMissingEngine = errors.New("missing engine in server configuration")

	errClientMustIdentify  = errors.New("client must identify")
	errCannotIdentifyAgain = errors.New("cannot identify again")
	errMissingNode         = errors.New("the node doesn't exist")
)

// The ServerConfig structure is used to configure nsqlookup servers.
type ServerConfig struct {
	// Engine to be used to implement the nsqlookup behavior.
	//
	// This field is required to be non-nil in order to succesfuly start a
	// nsqlookup server.
	Engine Engine

	// BroadcastAddress is the address advertized by the nsqlookup server.
	BroadcastAddress string

	// TcpAddress is the TCP address that the nsqlookup server will listen on.
	//
	// One of TcpAddress or TcpListener has to be set when starting a server.
	TcpAddress string

	// TcpListener is the TCP listener used by the nsqlookup server to accept
	// incoming connections.
	//
	// When set, the listener is owned by the nsqlookup server that manages it,
	// and will be closed when the server is stopped.
	//
	// One of TcpAddress or TcpListener has to be set when starting a server.
	TcpListener net.Listener

	// HttpAddress is the HTTP address that the nsqlookup server will listen on.
	//
	// One of HttpAddress or HttpListener has to be set when starting a server.
	HttpAddress string

	// HttpListener is the HTTP listener used by the nsqlookup server to accept
	// incoming connections.
	//
	// When set, the listener is owned by the nsqlookup server that manages it,
	// and will be closed when the server is stopped.
	//
	// One of HttpAddress or HttpListener has to be set when starting a server.
	HttpListener net.Listener

	// PingTimeout is the maximum duration the server will wait for nsqd nodes
	// to send ping commands before disconnecting them.
	PingTimeout time.Duration

	// ReadTimeout is the maximum duration the server will wait for read
	// operations to complete.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration the server will wait for write
	// operations to complete.
	WriteTimeout time.Duration
}

// Server is an opaque type that represents a nsqlookup server.
type Server struct {
	engine Engine

	address  string
	tcpLstn  net.Listener
	httpLstn net.Listener
	join     sync.WaitGroup
	mutex    sync.Mutex
	conns    map[net.Conn]bool

	pingTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// StartServer starts a new nsqlookup server s configured with config, returning
// an error if either the configuration was invalid or an runtime error occured.
func StartServer(config ServerConfig) (s *Server, err error) {
	var tcpLstn net.Listener
	var httpLstn net.Listener

	if config.Engine == nil {
		err = ErrMissingEngine
		return
	}

	if len(config.TcpAddress) == 0 {
		config.TcpAddress = DefaultTcpAddress
	}

	if len(config.HttpAddress) == 0 {
		config.HttpAddress = DefaultHttpAddress
	}

	if config.PingTimeout == 0 {
		config.PingTimeout = DefaultPingTimeout
	}

	if config.ReadTimeout == 0 {
		config.ReadTimeout = DefaultReadTimeout
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = DefaultWriteTimeout
	}

	if tcpLstn = config.TcpListener; tcpLstn == nil {
		if tcpLstn, err = net.Listen("tcp", config.TcpAddress); err != nil {
			return
		}
		defer func() {
			if tcpLstn != nil {
				tcpLstn.Close()
			}
		}()
	}

	if httpLstn = config.HttpListener; httpLstn == nil {
		if httpLstn, err = net.Listen("tcp", config.HttpAddress); err != nil {
			return
		}
		defer func() {
			if httpLstn != nil {
				httpLstn.Close()
			}
		}()
	}

	if len(config.BroadcastAddress) == 0 {
		config.BroadcastAddress = tcpLstn.Addr().String()
	}

	s = &Server{
		engine: config.Engine,

		address:  config.BroadcastAddress,
		tcpLstn:  tcpLstn,
		httpLstn: httpLstn,
		conns:    make(map[net.Conn]bool),

		pingTimeout:  config.PingTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
	}

	s.join.Add(2)
	go s.serveTcp(tcpLstn, &s.join)
	go s.serveHttp(httpLstn, &s.join)

	tcpLstn, httpLstn = nil, nil
	return
}

// Stop stops the server s.
//
// This method can safely be called multiple times and by multiple goroutines,
// it blocks until all the internal resources of the nsqlookup server have been
// released.
func (s *Server) Stop() {
	s.tcpLstn.Close()
	s.httpLstn.Close()
	s.engine.Close()
	s.join.Wait()
}

func (s *Server) addConn(conn net.Conn) {
	s.mutex.Lock()
	s.conns[conn] = true
	s.mutex.Unlock()
}

func (s *Server) removeConn(conn net.Conn) {
	s.mutex.Lock()
	if s.conns != nil {
		delete(s.conns, conn)
	}
	s.mutex.Unlock()
}

func (s *Server) closeConns() {
	s.mutex.Lock()

	// Gracefuly shutdown the connections, if possible only closes the read end
	// of the connection so in-flights responses aren't abruptly terminated.
	for conn := range s.conns {
		switch c := conn.(type) {
		case *net.TCPConn:
			c.CloseRead()
		case *net.UnixConn:
			c.CloseRead()
		default:
			c.Close()
		}
	}

	s.conns = nil
	s.mutex.Unlock()
}

func (s *Server) serveTcp(lstn net.Listener, join *sync.WaitGroup) {
	defer join.Done()
	defer lstn.Close()
	defer s.closeConns()

	for {
		conn, err := lstn.Accept()

		if err != nil {
			switch {
			case isTimeout(err):
				continue
			case isTemporary(err):
				continue
			}
			return
		}

		s.addConn(conn)
		s.join.Add(1)
		go s.serveConn(conn, &s.join)
	}
}

func (s *Server) serveConn(conn net.Conn, join *sync.WaitGroup) {
	var node NodeInfo

	defer join.Done()
	defer conn.Close()
	defer s.removeConn(conn)

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for done := false; !done; {
		var cmd Command
		var res Response
		var err error

		if cmd, err = s.readCommand(conn, r); err == nil {
			switch c := cmd.(type) {
			case Ping:
				res, err = s.ping(node)

			case Identify:
				node, res, err = s.identify(node, c.Info)

			case Register:
				res, err = s.register(node, c.Topic, c.Channel)

			case Unregister:
				res, err = s.unregister(node, c.Topic, c.Channel)
			}
		}

		if done = err != nil; done {
			switch e := err.(type) {
			case Error:
				res = e
			default:
				res = Error{Code: ErrInvalid, Reason: err.Error()}
			}
		}

		if err = s.writeResponse(conn, w, res); err != nil {
			break
		}
	}
}

func (s *Server) identify(node NodeInfo, info NodeInfo) (id NodeInfo, res RawResponse, err error) {
	if node != (NodeInfo{}) {
		id, err = node, errCannotIdentifyAgain
		return
	}

	self, _ := s.engine.LookupInfo()

	_, sp1, _ := net.SplitHostPort(s.tcpLstn.Addr().String())
	_, sp2, _ := net.SplitHostPort(s.httpLstn.Addr().String())

	p1, _ := strconv.Atoi(sp1)
	p2, _ := strconv.Atoi(sp2)

	h, _ := os.Hostname()
	b, _ := json.Marshal(NodeInfo{
		BroadcastAddress: s.address,
		Hostname:         h,
		TcpPort:          p1,
		HttpPort:         p2,
		Version:          self.Version,
	})

	id, res = info, RawResponse(b)
	return
}

func (s *Server) ping(node NodeInfo) (res OK, err error) {
	if node != (NodeInfo{}) { // ping may arrive before identify
		err = s.engine.PingNode(node)
	}
	return
}

func (s *Server) register(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = s.engine.RegisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = s.engine.RegisterTopic(node, topic)

	default:
		err = s.engine.RegisterNode(node)
	}

	return
}

func (s *Server) unregister(node NodeInfo, topic string, channel string) (res OK, err error) {
	if node == (NodeInfo{}) {
		err = errClientMustIdentify
		return
	}

	switch {
	case len(channel) != 0:
		err = s.engine.UnregisterChannel(node, topic, channel)

	case len(topic) != 0:
		err = s.engine.UnregisterTopic(node, topic)

	default:
		err = s.engine.UnregisterNode(node)
	}

	return
}

func (s *Server) readCommand(c net.Conn, r *bufio.Reader) (cmd Command, err error) {
	if err = c.SetReadDeadline(time.Now().Add(s.pingTimeout)); err == nil {
		cmd, err = ReadCommand(r)
	}
	return
}

func (s *Server) writeResponse(c net.Conn, w *bufio.Writer, r Response) (err error) {
	if err = c.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err == nil {
		if err = r.Write(w); err == nil {
			err = w.Flush()
		}
	}
	return
}

func (s *Server) serveHttp(lstn net.Listener, join *sync.WaitGroup) {
	defer join.Done()
	defer lstn.Close()
	(&http.Server{
		Handler:      s,
		ReadTimeout:  s.readTimeout,
		WriteTimeout: s.writeTimeout,
	}).Serve(lstn)
	return
}

func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/lookup":
		s.serveLookup(res, req)

	case "/topics":
		s.serveTopics(res, req)

	case "/channels":
		s.serveChannels(res, req)

	case "/nodes":
		s.serveNodes(res, req)

	case "/ping":
		s.servePing(res, req)

	case "/info":
		s.serveInfo(res, req)

	case "/topic/delete", "/delete_topic":
		s.serveDeleteTopic(res, req)

	case "/channel/delete", "/delete_channel":
		s.serveDeleteChannel(res, req)

	case "/tombstone_topic_producer":
		s.serveTombstoneTopicProducer(res, req)

	default:
		s.sendNotFound(res)
	}
}

func (s *Server) serveLookup(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	nodes, err := s.engine.LookupProducers(topic)
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{sortedNodes(nodes)})
}

func (s *Server) serveTopics(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	topics, err := s.engine.LookupTopics()
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", struct {
		Topics []string `json:"topics"`
	}{sortedStrings(topics)})
}

func (s *Server) serveChannels(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	channels, err := s.engine.LookupChannels(topic)
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", struct {
		Channels []string `json:"channels"`
	}{sortedStrings(channels)})
}

func (s *Server) serveNodes(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	nodes, err := s.engine.LookupNodes()
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", struct {
		Producers []NodeInfo `json:"producers"`
	}{sortedNodes(nodes)})
}

func (s *Server) servePing(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	if err := s.engine.CheckHealth(); err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendOK(res)
}

func (s *Server) serveInfo(res http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	info, err := s.engine.LookupInfo()
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", info)
}

func (s *Server) serveDeleteTopic(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_topic") {
		s.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")

	if len(topic) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	info, err := s.engine.LookupInfo()
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	nodes, err := s.engine.LookupProducers(topic)
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	errChan := make(chan error, len(nodes))
	userAgent := "nsqlookupd/" + info.Version

	for _, node := range nodes {
		go func(client nsq.Client) { errChan <- client.DeleteTopic(topic) }(nsq.Client{
			Client:    http.Client{Timeout: s.readTimeout + s.writeTimeout},
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
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", nil)
}

func (s *Server) serveDeleteChannel(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && !(req.Method == "GET" && req.URL.Path == "/delete_channel") {
		s.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	channel := query.Get("channel")

	if len(topic) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(channel) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_CHANNEL", nil)
		return
	}

	info, err := s.engine.LookupInfo()
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	nodes, err := s.engine.LookupProducers(topic)
	if err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	errChan := make(chan error, len(nodes))
	userAgent := "nsqlookupd/" + info.Version

	for _, node := range nodes {
		go func(client nsq.Client) { errChan <- client.DeleteChannel(topic, channel) }(nsq.Client{
			Client:    http.Client{Timeout: s.readTimeout + s.writeTimeout},
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
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", nil)
}

func (s *Server) serveTombstoneTopicProducer(res http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" && req.Method != "GET" {
		s.sendMethodNotAllowed(res)
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	node := query.Get("node")

	if len(topic) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if len(node) == 0 {
		s.sendResponse(res, 500, "MISSING_ARG_NODE", nil)
		return
	}

	host, port, err := net.SplitHostPort(node)
	if err != nil {
		s.sendResponse(res, 500, "BAD_ARG_NODE", nil)
		return
	}

	intport, err := strconv.Atoi(port)
	if err != nil {
		s.sendResponse(res, 500, "BAD_ARG_NODE", nil)
		return
	}

	if err := s.engine.TombstoneNode(NodeInfo{
		BroadcastAddress: host,
		HttpPort:         intport,
	}, topic); err != nil {
		s.sendInternalServerError(res, err)
		return
	}

	s.sendResponse(res, 200, "OK", nil)
}

func (s *Server) sendResponse(res http.ResponseWriter, status int, text string, value interface{}) {
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

func (s *Server) sendOK(res http.ResponseWriter) {
	res.WriteHeader(http.StatusOK)
	res.Write([]byte("OK"))
}

func (s *Server) sendNotFound(res http.ResponseWriter) {
	s.sendError(res, 404, "NOT_FOUND")
}

func (s *Server) sendMethodNotAllowed(res http.ResponseWriter) {
	s.sendError(res, 405, "METHOD_NOT_ALLOWED")
}

func (s *Server) sendInternalServerError(res http.ResponseWriter, err error) {
	s.sendError(res, 500, err.Error())
}

func (s *Server) sendError(res http.ResponseWriter, status int, message string) {
	res.WriteHeader(status)
	json.NewEncoder(res).Encode(struct {
		Message string `json:"message"`
	}{message})
}
