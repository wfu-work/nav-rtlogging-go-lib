package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// TcpClient represents a TCP client connection.
type TcpClient struct {
	Host         string
	Port         int
	Extra        string
	DialTimeout  time.Duration
	conn         net.Conn
	connMu       sync.RWMutex
	startMu      sync.Mutex
	callbackMu   sync.RWMutex
	writeMu      sync.Mutex
	dial         func(context.Context, string, string, time.Duration) (net.Conn, error)
	dialMu       sync.Mutex
	dialCancel   context.CancelFunc
	wg           sync.WaitGroup
	onConnect    OnConnectFunc
	onDisConnect DisConnectFunc
	onData       OnDataFunc
	netError     NetErrorFunc
}

func (c *TcpClient) getConn() net.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *TcpClient) replaceConn(conn net.Conn) net.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	old := c.conn
	c.conn = conn
	return old
}

func (c *TcpClient) closeConnIfCurrent(conn net.Conn) bool {
	c.connMu.Lock()
	wasCurrent := c.conn == conn
	if wasCurrent {
		c.conn = nil
	}
	c.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	return wasCurrent
}

func (c *TcpClient) OnConnect(f OnConnectFunc) {
	c.callbackMu.Lock()
	c.onConnect = f
	c.callbackMu.Unlock()
}

func (c *TcpClient) DisConnect(f DisConnectFunc) {
	c.callbackMu.Lock()
	c.onDisConnect = f
	c.callbackMu.Unlock()
}

func (c *TcpClient) OnData(f OnDataFunc) {
	c.callbackMu.Lock()
	c.onData = f
	c.callbackMu.Unlock()
}

func (c *TcpClient) NetError(f NetErrorFunc) {
	c.callbackMu.Lock()
	c.netError = f
	c.callbackMu.Unlock()
}

func (c *TcpClient) callbacks() (OnConnectFunc, DisConnectFunc, OnDataFunc, NetErrorFunc) {
	c.callbackMu.RLock()
	defer c.callbackMu.RUnlock()
	return c.onConnect, c.onDisConnect, c.onData, c.netError
}

// NewTcpClient creates a new TcpClient.
func NewTcpClient(host string, port int) *TcpClient {
	return &TcpClient{Host: host, Port: port, DialTimeout: 30 * time.Second, dial: dialTCP}
}

func dialTCP(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}

// Start connects to the configured endpoint. Calling Start again replaces the
// previous connection.
func (c *TcpClient) Start() error {
	return c.Connect(context.Background())
}

// Connect establishes the TCP connection using ctx for cancellation.
func (c *TcpClient) Connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.startMu.Lock()
	timeout := c.DialTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	address := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	dial := c.dial
	if dial == nil {
		dial = dialTCP
	}
	dialCtx, cancel := context.WithCancel(ctx)
	c.dialMu.Lock()
	c.dialCancel = cancel
	c.dialMu.Unlock()
	conn, err := dial(dialCtx, "tcp", address, timeout)
	c.dialMu.Lock()
	c.dialCancel = nil
	c.dialMu.Unlock()
	cancel()
	if err != nil {
		c.startMu.Unlock()
		_, _, _, onError := c.callbacks()
		if onError != nil {
			onError(err)
		}
		return fmt.Errorf("dial tcp server %s: %w", address, err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(2 * time.Minute)
	}
	if old := c.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}
	onConnect, _, _, _ := c.callbacks()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.handleConn(conn)
	}()
	c.startMu.Unlock()
	if onConnect != nil {
		onConnect(conn)
	}
	return nil
}

func (c *TcpClient) handleConn(conn net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			wasCurrent := c.closeConnIfCurrent(conn)
			if !wasCurrent {
				return
			}
			_, onDisconnect, _, onError := c.callbacks()
			if onDisconnect != nil {
				onDisconnect(conn)
			}
			if err != io.EOF && !errors.Is(err, net.ErrClosed) && onError != nil {
				onError(err)
			}
			return
		}
		_, _, onData, _ := c.callbacks()
		if onData != nil {
			data := append([]byte(nil), buf[:n]...)
			onData(conn, data)
		}
	}
}

// Stop closes the current connection. It is safe to call repeatedly.
func (c *TcpClient) Stop() error {
	c.dialMu.Lock()
	cancel := c.dialCancel
	c.dialMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.startMu.Lock()
	defer c.startMu.Unlock()
	conn := c.replaceConn(nil)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Shutdown stops the client and waits for its read goroutine to exit.
func (c *TcpClient) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := c.Stop()
	if waitErr := waitGroupContext(ctx, &c.wg); waitErr != nil {
		return waitErr
	}
	return err
}

// Write sends all bytes or returns an error.
func (c *TcpClient) Write(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.getConn()
	if conn == nil {
		return errors.New("tcp client not connected")
	}
	if err := writeAll(conn, data); err != nil {
		return fmt.Errorf("tcp connection write: %w", err)
	}
	return nil
}

// WriteData writes the provided string as-is. Despite the legacy parameter
// name, it does not decode hexadecimal text. New code should prefer Write.
func (c *TcpClient) WriteData(data string) error {
	return c.Write([]byte(data))
}

func writeAll(conn net.Conn, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := conn.Write(data[written:])
		written += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
