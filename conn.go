package nsq

import (
	"bufio"
	"net"
	"sync"
	"time"
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

	if conn, err = net.DialTimeout("tcp", addr, timeout); err != nil {
		return
	}

	c = NewConn(conn)
	return
}

func (c *Conn) WriteFrame(f Frame) (err error) {
	c.wmtx.Lock()
	if err = f.Write(c.wbuf); err == nil {
		err = c.wbuf.Flush()
	}
	c.wmtx.Unlock()
	return
}

func (c *Conn) Write(b []byte) (n int, err error) {
	c.wmtx.Lock()
	n, err = c.conn.Write(b)
	c.wmtx.Unlock()
	return
}

func (c *Conn) ReadFrame() (f Frame, err error) {
	c.rmtx.Lock()
	f, err = ReadFrame(c.rbuf)
	c.rmtx.Unlock()
	return
}

func (c *Conn) Read(b []byte) (n int, err error) {
	c.rmtx.Lock()
	n, err = c.conn.Read(b)
	c.rmtx.Unlock()
	return
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
