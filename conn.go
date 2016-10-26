package nsq

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	UserAgent = "github.com/segmentio/nsq-go"
)

type Conn struct {
	conn net.Conn

	rmtx sync.Mutex
	rbuf *bufio.Reader

	wmtx sync.Mutex
	wbuf *bufio.Writer
}

func NewConn(conn net.Conn) *Conn {
	const bufSize = 4096
	return &Conn{
		conn: conn,
		rbuf: bufio.NewReaderSize(conn, bufSize),
		wbuf: bufio.NewWriterSize(conn, bufSize),
	}
}

func Dial(addr string) (c *Conn, err error) {
	return DialTimeout(addr, 10*time.Second)
}

func DialTimeout(addr string, timeout time.Duration) (c *Conn, err error) {
	var conn net.Conn
	var magic = [...]byte{' ', ' ', 'V', '2'}

	if conn, err = net.DialTimeout("tcp", addr, timeout); err != nil {
		err = errors.Wrap(err, "dialing tcp://"+addr)
		return
	}

	if _, err = conn.Write(magic[:]); err != nil {
		conn.Close()
		err = errors.Wrap(err, "sending magic number to tcp://"+addr)
		return
	}

	c = NewConn(conn)
	return
}

func (c *Conn) WriteCommand(cmd Command) (err error) {
	c.wmtx.Lock()

	if err = cmd.Write(c.wbuf); err == nil {
		if err = c.wbuf.Flush(); err != nil {
			err = errors.Wrap(err, "flushing "+cmd.Name()+" command to "+c.RemoteAddr().String())
		}
	}

	c.wmtx.Unlock()
	return
}

func (c *Conn) WriteFrame(frame Frame) (err error) {
	c.wmtx.Lock()

	if err = frame.Write(c.wbuf); err == nil {
		if err = c.wbuf.Flush(); err != nil {
			err = errors.Wrap(err, "flushing "+frame.FrameType().String()+" frame to "+c.RemoteAddr().String())
		}
	}

	c.wmtx.Unlock()
	return
}

func (c *Conn) Write(b []byte) (n int, err error) {
	c.wmtx.Lock()

	if n, err = c.conn.Write(b); err != nil {
		err = errors.Wrap(err, "writing raw data to "+c.RemoteAddr().String())
	}

	c.wmtx.Unlock()
	return
}

func (c *Conn) ReadCommand() (cmd Command, err error) {
	c.rmtx.Lock()

	if cmd, err = ReadCommand(c.rbuf); err != nil {
		err = errors.WithMessage(err, "reading command from "+c.RemoteAddr().String())
	}

	c.rmtx.Unlock()
	return
}

func (c *Conn) ReadFrame() (frame Frame, err error) {
	c.rmtx.Lock()

	if frame, err = ReadFrame(c.rbuf); err != nil {
		err = errors.Wrap(err, "reading frame from "+c.RemoteAddr().String())
	}

	c.rmtx.Unlock()
	return
}

func (c *Conn) Read(b []byte) (n int, err error) {
	c.rmtx.Lock()

	if n, err = c.conn.Read(b); err != nil {
		err = errors.Wrap(err, "reading raw data from "+c.RemoteAddr().String())
	}

	c.rmtx.Unlock()
	return
}

func (c *Conn) Close() (err error) {
	if err = c.conn.Close(); err != nil {
		err = errors.Wrap(err, "closing tcp connection to "+c.RemoteAddr().String())
	}
	return
}

func (c *Conn) SetReadDeadline(t time.Time) (err error) {
	if err = c.conn.SetReadDeadline(t); err != nil {
		err = errors.Wrap(err, "setting read deadline on tcp connection to "+c.RemoteAddr().String())
	}
	return
}

func (c *Conn) SetWriteDeadline(t time.Time) (err error) {
	if err = c.conn.SetWriteDeadline(t); err != nil {
		err = errors.Wrap(err, "setting write deadline on tcp connection to "+c.RemoteAddr().String())
	}
	return
}

func (c *Conn) SetDeadline(t time.Time) (err error) {
	if err = c.conn.SetDeadline(t); err != nil {
		err = errors.Wrap(err, "setting read/write deadline on tcp connection to "+c.RemoteAddr().String())
	}
	return
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
