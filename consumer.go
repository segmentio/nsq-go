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
	msgs chan Message  // messages read from the connections
	done chan struct{} // closed when the consumer is shutdown
	once sync.Once

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
	mtx   sync.Mutex
	join  sync.WaitGroup
	conns map[string](chan<- Command)
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

func StartConsumer(config ConsumerConfig) (c *Consumer, err error) {
	if len(config.Topic) == 0 {
		err = errors.New("creating a new consumer requires a non-empty topic")
		return
	}

	if len(config.Channel) == 0 {
		err = errors.New("creating a new consumer requires a non-empty channel")
		return
	}

	if config.MaxInFlight == 0 {
		config.MaxInFlight = DefaultMaxInFlight
	}

	if config.DialTimeout == 0 {
		config.DialTimeout = DefaultDialTimeout
	}

	if config.ReadTimeout == 0 {
		config.ReadTimeout = DefaultReadTimeout
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = DefaultWriteTimeout
	}

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

		conns: make(map[string](chan<- Command)),
	}

	go c.run()
	return
}

func (c *Consumer) Stop() {
	c.once.Do(c.stop)
}

func (c *Consumer) Messages() <-chan Message {
	return c.msgs
}

func (c *Consumer) stop() {
	close(c.done)
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
			c.close()
			c.join.Wait()
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
			c.conns[addr] = cmdChan
			c.join.Add(1)
			go c.runConn(addr, cmdChan)
		}
	}

	c.mtx.Unlock()
	return
}

func (c *Consumer) close() {
	c.mtx.Lock()

	for _, cmdChan := range c.conns {
		sendCommand(cmdChan, Cls{})
	}

	c.mtx.Unlock()
	return
}

func (c *Consumer) closeConn(addr string) {
	c.mtx.Lock()
	cmdChan := c.conns[addr]
	delete(c.conns, addr)
	c.mtx.Unlock()
	c.join.Done()
	closeCommand(cmdChan)
}

func (c *Consumer) runConn(addr string, cmdChan chan Command) {
	defer c.closeConn(addr)

	var conn *Conn
	var err error
	var rdy int

	if conn, err = DialTimeout(addr, c.dialTimeout); err != nil {
		log.Printf("failed to connect to %s: %s", addr, err)
		return
	}

	c.join.Add(1)
	go c.writeConn(conn, cmdChan)

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
	defer c.join.Done()
	defer conn.Close()
	defer closeCommand(cmdChan)

	for {
		select {
		case cmd := <-cmdChan:
			if err := c.writeConnCommand(conn, cmd); err != nil {
				log.Print(err)
				return
			}

		case <-c.done:
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
