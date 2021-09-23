package nsq

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

// ProducerConfig carries the different variables to tune a newly started
// producer.
type ProducerConfig struct {
	Address        string
	Topic          string
	MaxConcurrency int
	DialTimeout    time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func (c *ProducerConfig) defaults() {
	if len(c.Address) == 0 {
		c.Address = "localhost:4151"
	}

	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = DefaultMaxConcurrency
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

// Producer provide an abstraction around using direct connections to nsqd
// nodes to send messages.
type Producer struct {
	// Communication channels of the producer.
	reqs    chan ProducerRequest
	done    chan struct{}
	once    sync.Once
	join    sync.WaitGroup
	ok      uint32
	started bool

	// Immutable state of the producer.
	address      string
	topic        string
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// ProducerRequest are used to represent operations that are submitted to
// producers.
type ProducerRequest struct {
	Topic    string
	Message  []byte
	Response chan<- error
	Deadline time.Time
}

// NewProducer configures a new producer instance.
func NewProducer(config ProducerConfig) (p *Producer, err error) {
	config.defaults()

	p = &Producer{
		reqs:         make(chan ProducerRequest, config.MaxConcurrency),
		done:         make(chan struct{}),
		address:      config.Address,
		topic:        config.Topic,
		dialTimeout:  config.DialTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
	}

	return
}

// StartProducer starts and returns a new producer p, configured with the
// variables from the config parameter, or returning an non-nil error if
// some of the configuration variables were invalid.
func StartProducer(config ProducerConfig) (p *Producer, err error) {
	p, err = NewProducer(config)
	if err != nil {
		return
	}

	p.Start()
	return
}

// Start explicitly begins the producer in case it was initialized with
// NewProducer instead of StartProducer.
func (p *Producer) Start() {
	if p.started {
		panic("(*Producer).Start has already been called")
	}

	concurrency := cap(p.reqs)
	p.join.Add(concurrency)
	for i := 0; i != concurrency; i++ {
		go p.run()
	}

	p.started = true
}

// StopWithWait gracefully shuts down the producer, allowing all inflight
// requests to be sent and for all backend connections to be closed. Once those
// actions are complete, the wait group is signaled.
//
// It is safe to call the method, or Stop, multiple times and from multiple
// goroutines; they will all block until the producer has been completely
// shutdown. Note, though, that if Stop is called first, a subsequent call to
// StopWithWait will not allow for inflight requests to be sent.
func (p *Producer) StopWithWait(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()

		p.once.Do(func() {
			close(p.reqs)
		})
		p.join.Wait()
	}()
}

// Stop gracefully shuts down the producer, cancelling all inflight requests and
// waiting for all backend connections to be closed.
//
// It is safe to call the method, or StopWithWait, multiple times and from
// multiple goroutines; they will all block until the producer has been
// completely shutdown. Note, though, that if StopWithWait is called first, a
// subsequent call to Stop will not prevent inflight requests from being sent.
func (p *Producer) Stop() {
	p.once.Do(p.stop)
	err := errors.New("publishing to a producer that was already stopped")

	for req := range p.reqs {
		req.complete(err)
	}

	p.join.Wait()
}

// Publish sends a message using the producer p, returning an error if it was
// already closed or if an error occurred while publishing the message.
//
// Note that no retry is done internally, the producer will fail after the
// first unsuccessful attempt to publish the message. It is the responsibility
// of the caller to retry if necessary.
func (p *Producer) Publish(message []byte) (err error) {
	return p.PublishTo(p.topic, message)
}

// PublishTo sends a message to a specific topic using the producer p, returning
// an error if it was already closed or if an error occurred while publishing the
// message.
//
// Note that no retry is done internally, the producer will fail after the
// first unsuccessful attempt to publish the message. It is the responsibility
// of the caller to retry if necessary.
func (p *Producer) PublishTo(topic string, message []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("publishing to a producer that was already stopped")
		}
	}()

	if len(topic) == 0 {
		return errors.New("no topic set for publishing message")
	}

	response := make(chan error, 1)
	deadline := time.Now().Add(p.dialTimeout + p.readTimeout + p.writeTimeout)

	// Attempts to queue the request so one of the active connections can pick
	// it up.
	p.reqs <- ProducerRequest{
		Topic:    topic,
		Message:  message,
		Response: response,
		Deadline: deadline,
	}

	// This will always trigger, either if the connection was lost or if a
	// response was successfully sent.
	err = <-response
	return
}

// Requests returns a write-only channel that can be used to submit requests to p.
//
// This method is useful when the publish operation needs to be associated with
// other operations on channels in a select statement for example, or to publish
// in a non-blocking fashion.
func (p *Producer) Requests() chan<- ProducerRequest {
	return p.reqs
}

// Connected returns true if the producer has successfully established a
// connection to nsqd, false otherwise.
func (p *Producer) Connected() bool {
	return atomic.LoadUint32(&p.ok) != 0
}

func (p *Producer) stop() {
	close(p.done)
	close(p.reqs)
}

func (p *Producer) run() {
	var conn *Conn
	var reqChan <-chan ProducerRequest
	var resChan chan Frame
	var pending []ProducerRequest

	shutdown := func(err error) {
		atomic.StoreUint32(&p.ok, 0)

		if conn != nil {
			close(resChan)
			conn.Close()
			conn = nil
			reqChan = nil
			resChan = nil
			pending = completeAllProducerRequests(pending, err)
		}

		if err != nil {
			log.Printf("closing nsqd connection to %s: %s", p.address, err)
		}
	}

	connect := func() (err error) {
		log.Printf("opening nsqd connection to %s", p.address)

		if conn, err = DialTimeout(p.address, p.dialTimeout); err != nil {
			log.Printf("failed to connect to nsqd at %s: %s", p.address, err)
			return
		}

		reqChan = p.reqs
		resChan = make(chan Frame, 16)
		go p.flush(conn, resChan)

		atomic.StoreUint32(&p.ok, 1)
		return
	}

	defer p.join.Done()
	defer shutdown(nil)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	connect()

	for {
		select {
		case <-p.done:
			return

		case now := <-ticker.C:
			if conn == nil {
				connect()
			}

			if producerRequestsTimedOut(pending, now) {
				shutdown(errors.New("timeout"))
				continue
			}

		case req, ok := <-reqChan:
			if !ok && req.Topic == "" {
				// exit if reqChan is closed and there are no more requests to process
				return
			}

			if err := p.publish(conn, req.Topic, req.Message); err != nil {
				req.complete(err)
				shutdown(err)
				continue
			}

			pending = append(pending, req)

		case frame := <-resChan:
			switch f := frame.(type) {
			case Response:
				switch f {
				case OK:
					pending = completeNextProducerRequest(pending, nil)

				case Heartbeat:
					if err := p.ping(conn); err != nil {
						shutdown(err)
						continue
					}

				case CloseWait:
					return

				default:
					shutdown(errors.Errorf("closing connection after receiving an unexpected response from %s: %s", conn.RemoteAddr(), f))
					continue
				}

			case Error:
				shutdown(errors.Errorf("closing connection after receiving an error from %s: %s", conn.RemoteAddr(), f))
				continue

			case Message:
				shutdown(errors.Errorf("closing connection after receiving an unexpected message from %s: %s", conn.RemoteAddr(), f.FrameType()))
				continue

			default:
				shutdown(errors.Errorf("closing connection after receiving an unsupported frame from %s: %s", conn.RemoteAddr(), f.FrameType()))
				continue
			}
		}
	}
}

func (p *Producer) flush(conn *Conn, resChan chan<- Frame) {
	defer func() { recover() }() // may happen if the resChan is closed
	defer conn.Close()

	for {
		frame, err := conn.ReadFrame()

		if err != nil {
			resChan <- Error(err.Error())
			return
		}

		resChan <- frame
	}
}

func (p *Producer) write(conn *Conn, cmd Command) (err error) {
	if err = conn.SetWriteDeadline(time.Now().Add(p.writeTimeout)); err == nil {
		err = conn.WriteCommand(cmd)
	}
	return
}

func (p *Producer) publish(conn *Conn, topic string, message []byte) error {
	return p.write(conn, Pub{
		Topic:   topic,
		Message: message,
	})
}

func (p *Producer) ping(conn *Conn) error {
	return p.write(conn, Nop{})
}

func (r ProducerRequest) complete(err error) {
	if r.Response != nil {
		r.Response <- err
	}
}

func completeNextProducerRequest(reqs []ProducerRequest, err error) []ProducerRequest {
	reqs[0].complete(err)
	return reqs[1:]
}

func completeAllProducerRequests(reqs []ProducerRequest, err error) []ProducerRequest {
	for _, req := range reqs {
		req.complete(err)
	}
	return nil
}

func producerRequestsTimedOut(reqs []ProducerRequest, now time.Time) bool {
	return len(reqs) != 0 && now.After(reqs[0].Deadline)
}
