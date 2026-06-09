package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// TcpClient represents an Ntrip client.
type TcpClient struct {
	Host         string
	Port         int
	Extra        string
	conn         net.Conn
	connMu       sync.RWMutex
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

// OnConnect 设置连接回调
func (c *TcpClient) OnConnect(f OnConnectFunc) {
	c.onConnect = f
}

// DisConnect 设置断开回调
func (c *TcpClient) DisConnect(f DisConnectFunc) {
	c.onDisConnect = f
}

// OnData 设置断开回调
func (c *TcpClient) OnData(f OnDataFunc) {
	c.onData = f
}

// NetError 设置网络错误回调
func (c *TcpClient) NetError(f NetErrorFunc) {
	c.netError = f
}

// NewTcpClient creates a new TcpClient.
func NewTcpClient(host string, port int) *TcpClient {
	return &TcpClient{
		Host: host,
		Port: port,
	}
}

func (c *TcpClient) Start() error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", c.Host, c.Port), 30*time.Second)
	if err != nil {
		fmt.Println("❌tcp client start error: ", err, "exit!")
		if c.netError != nil {
			c.netError(err)
		}
		return err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(2 * time.Minute)
	}
	if old := c.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}
	if c.onConnect != nil {
		c.onConnect(conn)
	}
	go c.handleConn(conn)
	return nil
}

// handleConn 处理每个连接
func (c *TcpClient) handleConn(conn net.Conn) {
	defer c.closeConnIfCurrent(conn)

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("❌tcp client disconnected:", conn.RemoteAddr())
			}
			if c.onDisConnect != nil {
				c.onDisConnect(conn)
			}
			break
		}
		data := buf[:n]
		if c.onData != nil {
			c.onData(conn, data)
		}
	}
}

func (c *TcpClient) Stop() error {
	conn := c.getConn()
	if conn != nil {
		fmt.Println("tcp client stop ......")
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()
		return conn.Close()
	}
	return nil
}

func (c *TcpClient) WriteData(hexStr string) error {
	conn := c.getConn()
	if conn == nil {
		fmt.Printf("❌tcp发送数据异常: %s\n", "tcp client not connected")
		return errors.New("tcp client not connected")
	}
	_, err := conn.Write([]byte(hexStr))
	if err != nil {
		fmt.Printf("❌tcp发送数据异常: %v - %s\n", err, hexStr)
		return fmt.Errorf("❌tcp send data conn write error: %v", err)
	}
	fmt.Printf("✅tcp发送数据成功: %s\n", hexStr)
	return nil
}
