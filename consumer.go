package nsq

import (
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Consumer struct {
	// Communication channels of the consumer.
	msgs    chan Message  // messages read from the connections
	done    chan struct{} // closed when the consumer is shutdown
	once    sync.Once
	started bool

	// Immutable state of the consumer.
	topic        string
	channel      string
	address      string
	lookup       []string
	maxInFlight  int
	identify     Identify
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	// Shared state of the consumer.
	mtx  sync.Mutex
	join sync.WaitGroup
	shutJoin sync.WaitGroup
	//conns map[string](chan<- Command)
	conns    map[string]ConnMeta
	shutdown bool
}

type ConsumerConfig struct {
	Topic        string
	Channel      string
	Address      string
	Lookup       []string
	MaxInFlight  int
	Identify     Identify
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type ConnMeta struct {
	CmdChan chan<- Command
	Con     *Conn
}

// validate ensures that this configuration is well-formed.
func (c *ConsumerConfig) validate() error {
	if len(c.Topic) == 0 {
		return errors.New("creating a new consumer requires a non-empty topic")
	}

	if len(c.Channel) == 0 {
		return errors.New("creating a new consumer requires a non-empty channel")
	}

	return nil
}

// defaults will set up this configuration with the global defaults where they
// were not already set.
func (c *ConsumerConfig) defaults() {
	if c.MaxInFlight == 0 {
		c.MaxInFlight = DefaultMaxInFlight
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}

	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
}

// NewConsumer configures a new consumer instance.
func NewConsumer(config ConsumerConfig) (c *Consumer, err error) {
	if err = config.validate(); err != nil {
		return
	}

	config.defaults()

	c = &Consumer{
		msgs: make(chan Message, config.MaxInFlight),
		done: make(chan struct{}),

		topic:        config.Topic,
		channel:      config.Channel,
		address:      config.Address,
		lookup:       append([]string{}, config.Lookup...),
		maxInFlight:  config.MaxInFlight,
		identify:     setIdentifyDefaults(config.Identify),
		dialTimeout:  config.DialTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
		conns:        make(map[string]ConnMeta),
	}

	return
}

// StartConsumer creates and starts consuming from NSQ right away. This is the
// fastest way to get up and running.
func StartConsumer(config ConsumerConfig) (c *Consumer, err error) {
	c, err = NewConsumer(config)
	if err != nil {
		return
	}

	c.Start()
	return
}

// Start explicitly begins consumption in case the consumer was initialized
// with NewConsumer instead of StartConsumer.
func (c *Consumer) Start() {
	if c.started {
		panic("(*Consumer).Start has already been called")
	}

	go c.run()

	c.started = true
}

func (c *Consumer) Stop() {
	c.once.Do(c.stop)
}

func (c *Consumer) Messages() <-chan Message {
	return c.msgs
}

func (c *Consumer) stop() {
	c.shutJoin.Add(1)
	c.mtx.Lock()
	c.shutdown = true
	c.mtx.Unlock()
	close(c.done)
	c.shutJoin.Wait()
}

func (c *Consumer) run() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer close(c.msgs)

	if err := c.pulse(); err != nil {
		log.Print(err)
	}

	for {
		select {
		case <-ticker.C:
			if err := c.pulse(); err != nil {
				log.Print(err)
			}

		case <-c.done:
			log.Println("Consumer initiating shutdown sequence")
			c.close()
			log.Println("awaiting connection waitgroup")
			c.join.Wait()
			// drain and requeue any in-flight messages
			log.Println("draining and requeueing remaining in-flight messages")
			c.drainRemaining()
			log.Println("closing and cleaning up connections")
			c.mtx.Lock()
			for addr, cm := range c.conns {
				delete(c.conns, addr)
				for len(cm.CmdChan) > 0 {
					log.Println("awaiting for write channel to flush requeues")
					time.Sleep(time.Second)
				}
				closeCommand(cm.CmdChan)
				cm.Con.Close()
			}
			c.mtx.Unlock()
			log.Println("Consumer exiting run")
			c.shutJoin.Done()
			return
		}
	}
}

func (c *Consumer) drainRemaining() {
	for {
		select {
		case m, ok := <-c.msgs:
			if !ok {
				log.Printf("message channel closed, nothing to drain")
				return
			}
			log.Printf("requeueing %+v\n", m.ID.String())
			sendCommand(m.cmdChan, Req{MessageID: m.ID})
		default:
			return
		}
	}
}

func (c *Consumer) pulse() (err error) {
	var nodes []string

	if len(c.lookup) == 0 {
		nodes = []string{c.address}
	} else {
		var res LookupResult

		// Let the error propagate to the caller but if the result is not empty
		// we still want to process it.
		res, err = (&LookupClient{
			Client:    http.Client{Timeout: c.dialTimeout + c.readTimeout + c.writeTimeout},
			Addresses: c.lookup,
			UserAgent: c.identify.UserAgent,
		}).Lookup(c.topic)

		for _, p := range res.Producers {
			host, port, _ := net.SplitHostPort(p.BroadcastAddress)
			if len(host) == 0 {
				host = p.BroadcastAddress
			}
			if len(port) == 0 {
				port = strconv.Itoa(p.TcpPort)
			}
			nodes = append(nodes, net.JoinHostPort(host, port))
		}
	}

	c.mtx.Lock()

	for _, addr := range nodes {
		if _, exists := c.conns[addr]; !exists {
			// '+ 2' for the initial identify and subscribe commands.
			cmdChan := make(chan Command, c.maxInFlight+2)
			conn, err := c.getConn(addr)
			if err != nil {
				log.Printf("failed to connect to %s: %s", addr, err)
				continue
			}
			cm := ConnMeta{CmdChan: cmdChan, Con: conn}
			c.conns[addr] = cm
			c.join.Add(1)
			go c.runConn(conn, addr, cmdChan)
			go c.writeConn(conn, cmdChan)
		}
	}

	c.mtx.Unlock()
	return
}

func (c *Consumer) close() {
	log.Println("sending CLS to all command channels")
	c.mtx.Lock()
	for _, cm := range c.conns {
		sendCommand(cm.CmdChan, Cls{})
	}
	c.mtx.Unlock()
	return
}

func (c *Consumer) closeConn(addr string) {
	c.mtx.Lock()
	cm := c.conns[addr]
	if !c.shutdown {
		delete(c.conns, addr)
		closeCommand(cm.CmdChan)
		log.Println("closing socket")
		cm.Con.Close()
	}
	c.mtx.Unlock()
}

func (c *Consumer) getConn(addr string) (*Conn, error) {
	conn, err := DialTimeout(addr, c.dialTimeout)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *Consumer) runConn(conn *Conn, addr string, cmdChan chan Command) {
	defer c.closeConn(addr)
	defer c.join.Done()
	var rdy int

	sendCommand(cmdChan, c.identify)
	sendCommand(cmdChan, Sub{Topic: c.topic, Channel: c.channel})

	for {
		var frame Frame
		var err error

		if rdy == 0 {
			rdy = c.approximateRdyCount()
			sendCommand(cmdChan, Rdy{Count: rdy})
		}

		if frame, err = conn.ReadFrame(); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Print(err)
			}
			return
		}

		switch f := frame.(type) {
		case Message:
			f.cmdChan = cmdChan
			c.msgs <- f
			rdy--

		case Response:
			switch f {
			case OK:
			case Heartbeat:
				sendCommand(cmdChan, Nop{})

			case CloseWait:
				log.Println("closewait received exiting runConn")
				return

			default:
				log.Printf("closing connection after receiving an unexpected response from %s: %s", conn.RemoteAddr(), f)
				return
			}

		case Error:
			log.Printf("closing connection after receiving an error from %s: %s", conn.RemoteAddr(), f)
			return

		default:
			log.Printf("closing connection after receiving an unsupported frame from %s: %s", conn.RemoteAddr(), f.FrameType())
			return
		}
	}
}

func (c *Consumer) writeConn(conn *Conn, cmdChan chan Command) {
	for cmd := range cmdChan {
		if err := c.writeConnCommand(conn, cmd); err != nil {
			log.Print(err)
			return
		}
	}
}

func (c *Consumer) writeConnCommand(conn *Conn, cmd Command) (err error) {
	if err = conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return
	}
	err = conn.WriteCommand(cmd)
	return
}

func (c *Consumer) approximateRdyCount() (count int) {
	c.mtx.Lock()
	conns := len(c.conns)
	c.mtx.Unlock()

	if conns == 0 {
		count = 1
	} else {
		count = c.maxInFlight / conns
	}

	if count < 1 {
		count = 1
	}

	return
}

// RateLimit consumes messages from the messages channel and limits the rate at
// which they are produced to the channel returned by this function.
//
// The limit is the maximum number of messages per second that are produced.
// No rate limit is applied if limit is negative or zero.
//
// The returned channel is closed when the messages channel is closed.
func RateLimit(limit int, messages <-chan Message) <-chan Message {
	if limit <= 0 {
		return messages
	}

	output := make(chan Message)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		defer close(output)

		input := messages
		count := 0

		for {
			select {
			case <-ticker.C:
				count = 0
				input = messages

			case msg, ok := <-input:
				if !ok {
					return
				}

				output <- msg

				if count++; count >= limit {
					input = nil
				}
			}
		}
	}()

	return output
}
