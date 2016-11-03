package nsqlookup

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	nsq "github.com/segmentio/nsq-go"
)

const (
	DefaultReadTimeout  = 5 * time.Second
	DefaultWriteTimeout = 5 * time.Second
)

var (
	ErrMissingEngine      = errors.New("missing engine in server configuration")
	ErrMissingTcpAddress  = errors.New("missing tcp address in server configuration")
	ErrMissingHttpAddress = errors.New("missing http address in server configuration")
)

type ServerConfig struct {
	Engine           Engine
	BroadcastAddress string

	TcpAddress  string
	TcpListener net.Listener

	HttpAddress  string
	HttpListener net.Listener

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type Server struct {
	engine Engine

	tcpLstn  net.Listener
	httpLstn net.Listener
	join     sync.WaitGroup

	readTimeout  time.Duration
	writeTimeout time.Duration
}

func StartServer(config ServerConfig) (s *Server, err error) {
	var tcpLstn net.Listener
	var httpLstn net.Listener

	if config.Engine == nil {
		err = ErrMissingEngine
		return
	}

	if len(config.TcpAddress) == 0 && config.TcpListener == nil {
		err = ErrMissingTcpAddress
		return
	}

	if len(config.HttpAddress) == 0 && config.HttpListener == nil {
		err = ErrMissingHttpAddress
		return
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
			if s == nil {
				tcpLstn.Close()
			}
		}()
	}

	if httpLstn = config.HttpListener; httpLstn == nil {
		if httpLstn, err = net.Listen("tcp", config.HttpAddress); err != nil {
			return
		}
		defer func() {
			if s == nil {
				httpLstn.Close()
			}
		}()
	}

	s = &Server{
		engine: config.Engine,

		tcpLstn:  tcpLstn,
		httpLstn: httpLstn,

		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
	}

	s.join.Add(2)
	go s.serveTcp(tcpLstn, &s.join)
	go s.serveHttp(httpLstn, &s.join)
	return
}

func (s *Server) Stop() {
	s.tcpLstn.Close()
	s.httpLstn.Close()
	s.join.Wait()
}

func (s *Server) serveTcp(lstn net.Listener, join *sync.WaitGroup) {
	defer join.Done()
	defer lstn.Close()

	for {
		conn, err := lstn.Accept()

		if err != nil {
			if e, ok := err.(interface {
				Timeout() bool
			}); ok && e.Timeout() {
				continue
			}

			if e, ok := err.(interface {
				Temporary() bool
			}); ok && e.Temporary() {
				continue
			}

			return
		}

		s.join.Add(1)
		go s.ServeConn(conn, &s.join)
	}
}

func (s *Server) ServeConn(conn net.Conn, join *sync.WaitGroup) {
	defer join.Done()
	defer conn.Close()
	// TODO
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
	}{nodes})
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
	}{topics})
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
	}{channels})
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
	}{nodes})
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

	_ = host
	_ = port

	// TODO
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
