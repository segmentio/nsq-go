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
	drainTimeout time.Duration

	// Shared state of the consumer.
	mtx      sync.Mutex
	join     sync.WaitGroup
	shutJoin sync.WaitGroup
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
	DrainTimeout time.Duration
}

// Helper struct to maintain a Conn and its associated Command channel
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
	if c.DrainTimeout == 0 {
		c.DrainTimeout = DefaultDrainTimeout
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
		drainTimeout: config.DrainTimeout,
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

// stop kicks off an orderly shutdown of the Consumer.
func (c *Consumer) stop() {
	// We add 1 to the shutJoin WaitGroup to block until our Consumer.run() routine has completed.
	// This ensures that we properly cleanup and requeue and in-flight messages before closing
	// connections and returning.
	c.shutJoin.Add(1)
	// Lock the state mutex and set shutdown to true.
	c.mtx.Lock()
	c.shutdown = true
	c.mtx.Unlock()
	// Kick off the shutdown logic in our Consumer.run() to initiate the <-c.done case
	close(c.done)
	// Await Consumer.run() <-c.done to complete
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
			// Send a CLS to all Cmd Channels for all connections
			c.close()
			log.Println("awaiting connection waitgroup")
			// Wait for all runConn routines to return
			c.join.Wait()
			// At this point all runConn routines have returned, therefore we know
			// we won't be receiving and new messages from nsqd servers. Now we can
			// begin the processes of draining any in-flight messages and issuing a
			// REQ command for each message.
			log.Println("draining and requeueing remaining in-flight messages")
			// drain and requeue any in-flight messages
			c.drainRemaining()
			log.Println("closing and cleaning up connections")
			// Cleanup remaining connections
			c.mtx.Lock()
			for addr, cm := range c.conns {
				delete(c.conns, addr)
				// At this point we have drained all Messages from our main msgs channel and
				// sent them REQ commands for each on their associated CmdChan. However, we can not simply just
				// close the CmdChan for each connection yet. These channels are buffered and if
				// we simply call closeCommand(cm.CmdChan) here there is a race, as the writeConn routines
				// may not have finished processing all the REQ commands.
				// Therefore we check the length of the channel and await for it to reach 0. If for some reason
				// it fails to drain after a number of attempts we continue on and allow the messages to simply timeout
				// and be reqeueued by the nsqd server.
				attempts := 0
				for len(cm.CmdChan) > 0 {
					// If we've tried to allow the messages to flush and they've failed after
					// 3 attempts we give up and let the nsqd instance timeout and requeue for us.
					if attempts > 2 {
						log.Printf("failed to requeue %d messages during orderly shutdown, allow them to timeout and requeue", len(cm.CmdChan))
						break
					}
					log.Println("awaiting for write channel to flush any requeue commands")
					time.Sleep(c.drainTimeout / 3)
					attempts++
				}
				closeCommand(cm.CmdChan)
				cm.Con.Close()
			}
			c.mtx.Unlock()
			log.Println("Consumer exiting run")
			// Signal to the stop() function that orderly shutdown is complete
			c.shutJoin.Done()
			return
		}
	}
}

// drainRemaining takes any remaining in-flight messages from the Consumer.msgs
// channel and issues a REQ command for each.
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
			log.Printf("no messages to drain and requeue")
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
	// If we're not in shutdown mode we want to properly delete and close
	// this connection. This could happen for any number of reasons including
	// nsq servers being removed or intermittent network failure. However, if we
	// are in shutdown mode, we know that an orderly shutdown is in process and we
	// wish to retain these connections while we cleanup. This will allow the Consumer.run()
	// routine the opportunity to drain and requeue remaining in-flight messages.
	// The Consumer.run() routine will then handle deleting, and closing the channels and
	// connections for us rather than doing it here.
	if !c.shutdown {
		delete(c.conns, addr)
		closeCommand(cm.CmdChan)
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
