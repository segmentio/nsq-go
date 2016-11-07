package nsq

import (
	"io"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// ProducerConfig carries the different variables to tune a newly started
// producer.
type ProducerConfig struct {
	Address         string
	Topic           string
	MaxConcurrency  int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxRetryTimeout time.Duration
	MinRetryTimeout time.Duration
}

// Producer provide an abstraction around using direct connections to nsqd
// nodes to send messages.
type Producer struct {
	// Communication channels of the producer.
	reqs chan ProducerRequest
	done chan struct{}
	once sync.Once
	join sync.WaitGroup

	// Immutable state of the producer.
	address         string
	topic           string
	dialTimeout     time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	maxRetryTimeout time.Duration
	minRetryTimeout time.Duration
}

// ProducerRequest are used to represent operations that are submitted to
// producers.
type ProducerRequest struct {
	Message  []byte
	Response chan<- error
}

// StartProducer starts and returns a new producer p, configured with the
// variables from the config parameter, or returning an non-nil error if
// some of the configuration variables were invalid.
func StartProducer(config ProducerConfig) (p *Producer, err error) {
	if len(config.Topic) == 0 {
		err = errors.New("creating a producer requires a non-empty topic")
		return
	}

	if len(config.Address) == 0 {
		config.Address = "localhost:4151"
	}

	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = DefaultMaxConcurrency
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

	if config.MaxRetryTimeout == 0 {
		config.MaxRetryTimeout = DefaultMaxRetryTimeout
	}

	if config.MinRetryTimeout == 0 {
		config.MinRetryTimeout = DefaultMinRetryTimeout
	}

	p = &Producer{
		reqs:            make(chan ProducerRequest, config.MaxConcurrency),
		done:            make(chan struct{}),
		address:         config.Address,
		topic:           config.Topic,
		dialTimeout:     config.DialTimeout,
		readTimeout:     config.ReadTimeout,
		writeTimeout:    config.WriteTimeout,
		maxRetryTimeout: config.MaxRetryTimeout,
		minRetryTimeout: config.MinRetryTimeout,
	}
	p.join.Add(config.MaxConcurrency)

	for i := 0; i != config.MaxConcurrency; i++ {
		go p.run()
	}

	return
}

// Stop gracefully shutsdown the producer, cancelling all inflight requests and
// waiting for all backend connections to be closed.
//
// It is safe to call the method multiple times and from multiple goroutines,
// they will all block until the producer has been completely shutdown.
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
	defer func() {
		if recover() != nil {
			err = errors.New("publishing to a producer that was already stopped")
		}
	}()

	res := make(chan error, 1)

	p.reqs <- ProducerRequest{
		Message:  message,
		Response: res,
	}

	err = <-res
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

func (p *Producer) stop() {
	close(p.done)
	close(p.reqs)
}

func (p *Producer) run() {
	var conn *Conn
	var pipe chan ProducerRequest
	var ping chan struct{}
	var retry time.Duration

	shutdown := func() {
		if conn != nil {
			close(pipe)
			close(ping)
			conn.Close()
			conn = nil
			ping = nil
			pipe = nil
		}
	}

	defer p.join.Done()
	defer shutdown()

	for {
		select {
		case <-p.done:
			return

		case <-ping:
			if err := p.ping(conn); err != nil {
				shutdown()
				continue
			}

		case req, ok := <-p.reqs:
			if !ok {
				return
			}

			if conn == nil {
				var err error

				if conn, err = DialTimeout(p.address, p.dialTimeout); err != nil {
					req.complete(err)
					log.Printf("failed to connect to %s, retrying after %s: %s", p.address, retry, err)
					retry = p.sleep(retry)
					continue
				}

				retry = 0
				pipe = make(chan ProducerRequest)
				ping = make(chan struct{})
				go p.flush(conn, pipe, ping)
			}

			if err := p.publish(conn, req.Message); err != nil {
				shutdown()
				continue
			}

			pipe <- req
		}
	}
}

func (p *Producer) flush(conn *Conn, pipe <-chan ProducerRequest, ping chan<- struct{}) {
	var cnt int
	var err error

	defer conn.Close()
	defer func() { recover() }() // may happen when the ping channel is closed
	defer func() {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		for req := range pipe {
			req.Response <- err
		}
	}()

	for {
		var frame Frame

		if err = conn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return
		}

		if frame, err = conn.ReadFrame(); err != nil {
			if len(pipe) == 0 {
				continue
			}

			// After two consecutive timeouts and when there are pending
			// requests we assume the response will not come and we're better
			// off closing the connection and returning an error.
			if isTimeout(err) {
				if cnt++; cnt < 2 {
					continue
				}
			}

			return
		}

		cnt = 0

		switch f := frame.(type) {
		case Response:
			switch f {
			case OK:
				req := <-pipe
				req.complete(nil)
				continue

			case Heartbeat:
				ping <- struct{}{}
				continue

			case CloseWait:
				continue
			}

		case Error:
			err = errors.Errorf("closing connection after receiving an error from %s: %s", conn.RemoteAddr(), f)
			return

		case Message:
			err = errors.Errorf("closing connection after receiving an unexpected message from %s: %s", conn.RemoteAddr(), f.FrameType())
			return

		default:
			err = errors.Errorf("closing connection after receiving an unsupported frame from %s: %s", conn.RemoteAddr(), f.FrameType())
			return
		}
	}
}

func (p *Producer) publish(conn *Conn, message []byte) error {
	return p.write(conn, Pub{Topic: p.topic, Message: message})
}

func (p *Producer) ping(conn *Conn) error {
	return p.write(conn, Nop{})
}

func (p *Producer) write(conn *Conn, cmd Command) (err error) {
	if err = conn.SetDeadline(time.Now().Add(p.writeTimeout)); err == nil {
		err = conn.WriteCommand(cmd)
	}
	return
}

func (p *Producer) sleep(d time.Duration) time.Duration {
	if d < p.minRetryTimeout {
		d = p.minRetryTimeout
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-p.done:
	}

	if d *= 2; d > p.maxRetryTimeout {
		d = p.maxRetryTimeout
	}

	return d
}

func (r ProducerRequest) complete(err error) {
	if r.Response != nil {
		r.Response <- err
	}
}
