package ntrip

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// NtripClient represents an Ntrip client.
type NtripClient struct {
	Host               string
	Port               int
	Mount              string
	Username           string
	Password           string
	IsGga              bool
	GgaTime            int
	Longitude          float64
	Latitude           float64
	Altitude           float64
	Extra              string
	conn               net.Conn
	connMu             sync.RWMutex
	onConnect          OnConnectFunc
	onDisConnect       OnDisConnectFunc
	onDataCallback     OnDataFunc
	onNetErrorCallback OnNetErrorFunc
	retrying           atomic.Bool
	quit               chan struct{}
}

func (c *NtripClient) getConn() net.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *NtripClient) setConn(conn net.Conn) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.conn = conn
}

func (c *NtripClient) replaceConn(conn net.Conn) net.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	old := c.conn
	c.conn = conn
	return old
}

func (c *NtripClient) clearConnIfCurrent(conn net.Conn) bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == conn {
		c.conn = nil
		return true
	}
	return false
}

func (c *NtripClient) closeConnIfCurrent(conn net.Conn) bool {
	wasCurrent := c.clearConnIfCurrent(conn)
	if conn != nil {
		_ = conn.Close()
	}
	return wasCurrent
}

func (c *NtripClient) closeConn() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
}

// OnConnect 设置连接回调
func (c *NtripClient) OnConnect(f OnConnectFunc) {
	c.onConnect = f
}

// DisConnect 设置断开回调
func (c *NtripClient) DisConnect(f OnDisConnectFunc) {
	c.onDisConnect = f
}

// OnNetErrorCallback 设置断开回调
func (c *NtripClient) OnNetErrorCallback(f OnNetErrorFunc) {
	c.onNetErrorCallback = f
}

// OnDataCallback 设置断开回调
func (c *NtripClient) OnDataCallback(f OnDataFunc) {
	c.onDataCallback = f
}

// NewLocalNtripClient creates a new NtripClient.
func NewLocalNtripClient(mount string) *NtripClient {
	return &NtripClient{
		Host:     "127.0.0.1",
		Port:     9095,
		Mount:    mount,
		Username: mount,
		Password: mount,
		IsGga:    false,
		GgaTime:  5,
		quit:     make(chan struct{}),
	}
}

// NewNtripClient creates a new NtripClient.
func NewNtripClient(host string, port int, mount string, username string, password string) *NtripClient {
	return &NtripClient{
		Host:     host,
		Port:     port,
		Mount:    mount,
		Username: username,
		Password: password,
		IsGga:    false,
		GgaTime:  5,
		quit:     make(chan struct{}),
	}
}

// NewNtripClientExtra creates a new NtripClient.
func NewNtripClientExtra(host string, port int, mount string, username string, password string, extra string) *NtripClient {
	return &NtripClient{
		Host:     host,
		Port:     port,
		Mount:    mount,
		Username: username,
		Password: password,
		IsGga:    false,
		GgaTime:  5,
		Extra:    extra,
		quit:     make(chan struct{}),
	}
}

// NewNtripClientGgaExtra creates a new NtripClient.
func NewNtripClientGgaExtra(host string, port int, mount string, username string, password string, latitude float64, longitude float64, altitude float64, extra string) *NtripClient {
	return &NtripClient{
		Host:      host,
		Port:      port,
		Mount:     mount,
		Username:  username,
		Password:  password,
		IsGga:     true,
		GgaTime:   1,
		Latitude:  latitude,
		Longitude: longitude,
		Altitude:  altitude,
		Extra:     extra,
		quit:      make(chan struct{}),
	}
}

func (c *NtripClient) Start() error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", c.Host, c.Port), 5*time.Second)
	if err != nil {
		log.Println("❌ntrip client start error: ", err, "exit!")
		if c.onNetErrorCallback != nil {
			c.onNetErrorCallback(err)
		}
		return err
	}
	enableTCPKeepAlive(conn)
	if old := c.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}
	authMsg := createNtripAuthMsg(c.Mount, c.Username, c.Password)
	_, err = conn.Write([]byte(authMsg))
	if err != nil {
		log.Println("❌ntrip client write error: ", err, "exit!")
		c.closeConnIfCurrent(conn)
		if c.onNetErrorCallback != nil {
			c.onNetErrorCallback(err)
		}
		return err
	}
	log.Println("✅ntrip client send auth msg for mount: ", c.Mount)
	go c.handleConn(conn)
	return nil
}

func (c *NtripClient) Stop() {
	safeClose(c.quit)
	c.closeConn()
}

func (c *NtripClient) Retry() {
	if c.quit != nil {
		select {
		case <-c.quit:
			return
		default:
		}
	}
	if !c.retrying.CompareAndSwap(false, true) {
		return
	}
	fmt.Printf("❌ntrip client %s-%d-%s-%s-%s 连接失败5秒后重试...\n", c.Host, c.Port, c.Mount, c.Username, maskSecret(c.Password))
	go func() {
		defer c.retrying.Store(false)
		ticker := time.NewTicker(time.Duration(5) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.quit:
				return
			case <-ticker.C:
				if c.getConn() != nil {
					return
				}
				err := c.Start()
				if err != nil {
					fmt.Printf("❌ntrip client %s-%d-%s-%s-%s 连接失败5秒后重试...\n", c.Host, c.Port, c.Mount, c.Username, maskSecret(c.Password))
					break
				}
				return
			}
		}
	}()
}

func (c *NtripClient) handleConn(conn net.Conn) {
	connDone := make(chan struct{})
	defer close(connDone)

	var lastDataTime atomic.Int64
	lastDataTime.Store(time.Now().UnixNano())
	var connected atomic.Bool
	var disconnectOnce sync.Once
	var responseBuf []byte
	notifyDisconnect := func() {
		if !connected.Load() {
			return
		}
		disconnectOnce.Do(func() {
			if c.onDisConnect != nil {
				c.onDisConnect(conn.LocalAddr().String(), c.Mount)
			}
		})
	}
	startGGA := func() {
		if !c.IsGga {
			return
		}
		go func() {
			interval := time.Duration(c.GgaTime) * time.Second
			if interval <= 0 {
				interval = time.Second
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-c.quit:
					log.Println("🛑GGA发送停止:", c.Mount)
					return
				case <-connDone:
					log.Println("🛑GGA发送停止:", c.Mount)
					return
				case <-ticker.C:
					gga := GenerateGGA(c.Latitude, c.Longitude, c.Altitude)
					_, err := conn.Write([]byte(gga))
					if err != nil {
						log.Println("❌发送GGA数据失败:", err)
						if c.closeConnIfCurrent(conn) {
							notifyDisconnect()
						}
						return
					}
				}
			}
		}()
	}
	deliverData := func(data []byte) {
		if len(data) == 0 || c.onDataCallback == nil {
			return
		}
		c.onDataCallback(conn.LocalAddr().String(), c.Mount, cloneBytes(data), c.Extra)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.quit:
				log.Println("🛑ntrip client read time out quit signal received:", c.Mount)
				return
			case <-connDone:
				return
			case <-ticker.C:
				last := time.Unix(0, lastDataTime.Load())
				if time.Since(last) > defaultNtripClientIdleTimeout {
					log.Println("⏱️超过60秒未收到数据，断开连接:", c.Mount)
					if c.closeConnIfCurrent(conn) {
						notifyDisconnect()
					}
					return
				}
			}
		}
	}()

	buf := make([]byte, 1024)
	for {
		select {
		case <-c.quit:
			log.Println("🛑ntrip client quit signal received:", c.Mount)
			if c.closeConnIfCurrent(conn) {
				notifyDisconnect()
			}
			return
		default:
			n, err := conn.Read(buf)
			if err != nil {
				if err == io.EOF {
					if c.closeConnIfCurrent(conn) {
						notifyDisconnect()
					}
					return
				}
				log.Println("❌ntrip client error reading:", err)
				if strings.Contains(err.Error(), "use of closed network connection") {
					log.Printf("ℹ️连接已关闭（忽略重复错误）: %s\n", c.Mount)
					if c.clearConnIfCurrent(conn) {
						notifyDisconnect()
					}
					return
				}
				if c.closeConnIfCurrent(conn) {
					notifyDisconnect()
				}
				return
			}
			if n > 0 {
				lastDataTime.Store(time.Now().UnixNano())
			}
			data := buf[:n]

			if !connected.Load() {
				responseBuf = append(responseBuf, data...)
				if bytes.Contains(responseBuf, []byte("401 Unauthorized")) || bytes.Contains(responseBuf, []byte("Bad Request")) || bytes.Contains(responseBuf, []byte("Mount Point Is Not Exit")) {
					log.Println("❌ntrip client auth failed! ", c.Mount, c.Username, maskSecret(c.Password))
					c.closeConnIfCurrent(conn)
					return
				}
				headerEnd := findNtripResponseHeaderEnd(responseBuf)
				if headerEnd < 0 {
					if len(responseBuf) > defaultNtripMaxHeaderSize {
						log.Println("❌ntrip client auth response too large:", c.Mount)
						c.closeConnIfCurrent(conn)
						return
					}
					continue
				}
				header := responseBuf[:headerEnd]
				payload := responseBuf[headerEnd:]
				if !bytes.Contains(header, []byte("ICY 200 OK")) {
					log.Println("❌ntrip client auth unexpected response:", c.Mount, string(header))
					c.closeConnIfCurrent(conn)
					return
				}
				if connected.CompareAndSwap(false, true) {
					log.Println("✅ntrip client auth ok! ", c.Mount, c.Username, maskSecret(c.Password))
					c.setConn(conn)
					if c.onConnect != nil {
						c.onConnect(conn.LocalAddr().String(), c.Mount, conn)
					}
					startGGA()
				}
				deliverData(payload)
				responseBuf = nil
				continue
			}

			deliverData(data)
		}
	}
}

// createNtripAuthMsg creates a Ntrip authentication message.
func createNtripAuthMsg(mountPoint, username, password string) string {
	head := fmt.Sprintf("GET /%s HTTP/1.0\r\n", mountPoint)
	head += "User-Agent: NTRIP Client\r\n"
	if utf8.RuneCountInString(username) > 0 && utf8.RuneCountInString(password) > 0 {
		auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		head += fmt.Sprintf("Authorization: Basic %s\r\n", auth)
	}
	head += "Accept: */*\r\n"
	head += "\r\n"
	return head
}

// createNtripAuthMsgLib creates a Ntrip authentication message.
func createNtripAuthMsgLib(mountPoint, username, password string) string {
	head := fmt.Sprintf("GET /%s HTTP/1.0\r\n", mountPoint)
	head += "User-Agent: NTRIP RTKLIB/demo5_b34L\r\n"
	if utf8.RuneCountInString(username) > 0 && utf8.RuneCountInString(password) > 0 {
		auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		head += fmt.Sprintf("Authorization: Basic %s\r\n", auth)
	}
	head += "\r\n"
	return head
}

func findNtripResponseHeaderEnd(data []byte) int {
	if headerEnd := bytes.Index(data, []byte("\r\n\r\n")); headerEnd >= 0 {
		return headerEnd + 4
	}

	const icyLine = "ICY 200 OK\r\n"
	if !bytes.HasPrefix(data, []byte(icyLine)) {
		return -1
	}
	if len(data) == len(icyLine) {
		return -1
	}
	if looksLikeHeaderContinuation(data[len(icyLine):]) {
		return -1
	}
	return len(icyLine)
}

func looksLikeHeaderContinuation(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	line := data
	if lineEnd := bytes.Index(line, []byte("\r\n")); lineEnd >= 0 {
		line = line[:lineEnd]
	}
	if len(line) == 0 {
		return true
	}
	for _, b := range line {
		if b < 0x20 || b >= 0x7f {
			return false
		}
	}
	if bytes.Contains(line, []byte(":")) {
		return true
	}
	for _, b := range line {
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '-' {
			continue
		}
		return false
	}
	return len(line) < 64
}

func safeClose(ch chan struct{}) {
	defer func() { recover() }()
	if ch != nil {
		close(ch)
	}
}

func maskSecret(secret string) string {
	if secret == "" {
		return ""
	}
	if len(secret) <= 4 {
		return "****"
	}
	return secret[:2] + "****" + secret[len(secret)-2:]
}
