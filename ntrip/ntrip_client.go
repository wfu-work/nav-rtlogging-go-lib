package ntrip

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	DialTimeout        time.Duration
	TLSConfig          *tls.Config
	UseNtripV2         bool
	dial               func(string, int, time.Duration, *tls.Config) (net.Conn, error)
	conn               net.Conn
	connMu             sync.RWMutex
	onConnect          OnConnectFunc
	onDisConnect       OnDisConnectFunc
	onDataCallback     OnDataFunc
	onNetErrorCallback OnNetErrorFunc
	retrying           atomic.Bool
	quit               chan struct{}
	startMu            sync.Mutex
	lifecycleMu        sync.Mutex
	callbackMu         sync.RWMutex
}

const (
	defaultNtripRetryInitial = 5 * time.Second
	defaultNtripRetryMax     = time.Minute
)

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

func (c *NtripClient) beginRun() chan struct{} {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.quit == nil || isClosed(c.quit) {
		c.quit = make(chan struct{})
	}
	return c.quit
}

func (c *NtripClient) currentRun() chan struct{} {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.quit == nil {
		c.quit = make(chan struct{})
	}
	return c.quit
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// OnConnect 设置连接回调
func (c *NtripClient) OnConnect(f OnConnectFunc) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onConnect = f
}

// DisConnect 设置断开回调
func (c *NtripClient) DisConnect(f OnDisConnectFunc) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onDisConnect = f
}

// OnNetErrorCallback 设置断开回调
func (c *NtripClient) OnNetErrorCallback(f OnNetErrorFunc) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onNetErrorCallback = f
}

// OnDataCallback 设置断开回调
func (c *NtripClient) OnDataCallback(f OnDataFunc) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onDataCallback = f
}

func (c *NtripClient) connectCallback() OnConnectFunc {
	c.callbackMu.RLock()
	defer c.callbackMu.RUnlock()
	return c.onConnect
}

func (c *NtripClient) disconnectCallback() OnDisConnectFunc {
	c.callbackMu.RLock()
	defer c.callbackMu.RUnlock()
	return c.onDisConnect
}

func (c *NtripClient) dataCallback() OnDataFunc {
	c.callbackMu.RLock()
	defer c.callbackMu.RUnlock()
	return c.onDataCallback
}

func (c *NtripClient) notifyNetError(err error) {
	c.callbackMu.RLock()
	callback := c.onNetErrorCallback
	c.callbackMu.RUnlock()
	if callback != nil {
		callback(err)
	}
}

// NewLocalNtripClient creates a new NtripClient.
func NewLocalNtripClient(mount string) *NtripClient {
	return &NtripClient{
		Host:        "127.0.0.1",
		Port:        9095,
		Mount:       mount,
		Username:    mount,
		Password:    mount,
		IsGga:       false,
		GgaTime:     5,
		DialTimeout: 5 * time.Second,
		dial:        dialNtrip,
		quit:        make(chan struct{}),
	}
}

// NewNtripClient creates a new NtripClient.
func NewNtripClient(host string, port int, mount string, username string, password string) *NtripClient {
	return &NtripClient{
		Host:        host,
		Port:        port,
		Mount:       mount,
		Username:    username,
		Password:    password,
		IsGga:       false,
		GgaTime:     5,
		DialTimeout: 5 * time.Second,
		dial:        dialNtrip,
		quit:        make(chan struct{}),
	}
}

// NewNtripClientExtra creates a new NtripClient.
func NewNtripClientExtra(host string, port int, mount string, username string, password string, extra string) *NtripClient {
	return &NtripClient{
		Host:        host,
		Port:        port,
		Mount:       mount,
		Username:    username,
		Password:    password,
		IsGga:       false,
		GgaTime:     5,
		Extra:       extra,
		DialTimeout: 5 * time.Second,
		dial:        dialNtrip,
		quit:        make(chan struct{}),
	}
}

// NewNtripClientGgaExtra creates a new NtripClient.
func NewNtripClientGgaExtra(host string, port int, mount string, username string, password string, latitude float64, longitude float64, altitude float64, extra string) *NtripClient {
	return &NtripClient{
		Host:        host,
		Port:        port,
		Mount:       mount,
		Username:    username,
		Password:    password,
		IsGga:       true,
		GgaTime:     1,
		Latitude:    latitude,
		Longitude:   longitude,
		Altitude:    altitude,
		Extra:       extra,
		DialTimeout: 5 * time.Second,
		dial:        dialNtrip,
		quit:        make(chan struct{}),
	}
}

func (c *NtripClient) Start() error {
	c.startMu.Lock()
	if err := validateMount(c.Mount); err != nil {
		c.startMu.Unlock()
		c.notifyNetError(err)
		return err
	}
	quit := c.beginRun()
	dial := c.dial
	if dial == nil {
		dial = dialNtrip
	}
	conn, err := dial(c.Host, c.Port, c.DialTimeout, c.TLSConfig)
	if err != nil {
		c.startMu.Unlock()
		logPrintln("❌ntrip client start error: ", err, "exit!")
		c.notifyNetError(err)
		return err
	}
	enableTCPKeepAlive(conn)
	if old := c.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}
	authMsg := createNtripAuthMsg(c.Mount, c.Username, c.Password)
	if c.UseNtripV2 {
		authMsg = createNtripAuthMsgV2(c.Mount, c.Username, c.Password, ntripAddress(c.Host, c.Port))
	}
	if err = WriteData(conn, []byte(authMsg)); err != nil {
		logPrintln("❌ntrip client write error: ", err, "exit!")
		c.closeConnIfCurrent(conn)
		c.startMu.Unlock()
		c.notifyNetError(err)
		return err
	}
	logPrintln("✅ntrip client send auth msg for mount: ", c.Mount)
	go c.handleConnWithQuit(conn, quit)
	c.startMu.Unlock()
	return nil
}

func (c *NtripClient) Stop() {
	c.startMu.Lock()
	safeClose(c.currentRun())
	c.closeConn()
	c.startMu.Unlock()
}

func (c *NtripClient) Retry() {
	quit := c.currentRun()
	if isClosed(quit) {
		return
	}
	if !c.retrying.CompareAndSwap(false, true) {
		return
	}
	logPrintf("ntrip client %s-%d-%s will retry connection", c.Host, c.Port, c.Mount)
	go func() {
		defer c.retrying.Store(false)
		delay := defaultNtripRetryInitial
		timer := time.NewTimer(jitterRetryDelay(delay))
		defer timer.Stop()
		for {
			select {
			case <-quit:
				return
			case <-timer.C:
				if c.getConn() != nil {
					return
				}
				if err := c.Start(); err == nil {
					return
				}
				delay *= 2
				if delay > defaultNtripRetryMax {
					delay = defaultNtripRetryMax
				}
				logPrintf("ntrip client %s-%d-%s connection failed; retrying in %s", c.Host, c.Port, c.Mount, delay)
				timer.Reset(jitterRetryDelay(delay))
			}
		}
	}()
}

func jitterRetryDelay(delay time.Duration) time.Duration {
	spread := delay / 5
	if spread <= 0 {
		return delay
	}
	return delay - spread/2 + time.Duration(rand.Int63n(int64(spread)+1))
}

func (c *NtripClient) handleConn(conn net.Conn) {
	if c.getConn() == nil {
		c.setConn(conn)
	}
	c.handleConnWithQuit(conn, c.currentRun())
}

func (c *NtripClient) handleConnWithQuit(conn net.Conn, quit <-chan struct{}) {
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
			if callback := c.disconnectCallback(); callback != nil {
				callback(conn.LocalAddr().String(), c.Mount)
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
				case <-quit:
					logPrintln("🛑GGA发送停止:", c.Mount)
					return
				case <-connDone:
					logPrintln("🛑GGA发送停止:", c.Mount)
					return
				case <-ticker.C:
					gga := GenerateGGA(c.Latitude, c.Longitude, c.Altitude)
					if err := WriteData(conn, []byte(gga)); err != nil {
						logPrintln("❌发送GGA数据失败:", err)
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
		if len(data) == 0 {
			return
		}
		if callback := c.dataCallback(); callback != nil {
			callback(conn.LocalAddr().String(), c.Mount, cloneBytes(data), c.Extra)
		}
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				logPrintln("🛑ntrip client read time out quit signal received:", c.Mount)
				return
			case <-connDone:
				return
			case <-ticker.C:
				last := time.Unix(0, lastDataTime.Load())
				if time.Since(last) > defaultNtripClientIdleTimeout {
					logPrintln("⏱️超过60秒未收到数据，断开连接:", c.Mount)
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
		case <-quit:
			logPrintln("🛑ntrip client quit signal received:", c.Mount)
			if c.closeConnIfCurrent(conn) {
				notifyDisconnect()
			}
			return
		default:
			if !connected.Load() {
				setReadDeadline(conn, defaultNtripAuthTimeout)
			}
			n, err := conn.Read(buf)
			if err != nil {
				if err == io.EOF {
					wasCurrent := c.closeConnIfCurrent(conn)
					if wasCurrent {
						notifyDisconnect()
					}
					if wasCurrent && !connected.Load() && !isClosed(quit) {
						c.notifyNetError(io.EOF)
					}
					return
				}
				logPrintln("❌ntrip client error reading:", err)
				if errors.Is(err, net.ErrClosed) {
					logPrintf("ℹ️连接已关闭（忽略重复错误）: %s\n", c.Mount)
					if c.clearConnIfCurrent(conn) {
						notifyDisconnect()
					}
					return
				}
				wasCurrent := c.closeConnIfCurrent(conn)
				if wasCurrent {
					notifyDisconnect()
				}
				if wasCurrent && !connected.Load() && !isClosed(quit) {
					c.notifyNetError(fmt.Errorf("read ntrip authentication response: %w", err))
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
					logPrintln("❌ntrip client auth failed! ", c.Mount, c.Username, maskSecret(c.Password))
					if c.closeConnIfCurrent(conn) {
						c.notifyNetError(fmt.Errorf("ntrip authentication failed for mount %q", c.Mount))
					}
					return
				}
				headerEnd := findNtripResponseHeaderEnd(responseBuf)
				if headerEnd < 0 {
					if len(responseBuf) > defaultNtripMaxHeaderSize {
						logPrintln("❌ntrip client auth response too large:", c.Mount)
						if c.closeConnIfCurrent(conn) {
							c.notifyNetError(fmt.Errorf("ntrip authentication response exceeds %d bytes", defaultNtripMaxHeaderSize))
						}
						return
					}
					continue
				}
				header := responseBuf[:headerEnd]
				payload := responseBuf[headerEnd:]
				if !isNtripSuccessHeader(header) {
					logPrintln("❌ntrip client auth unexpected response:", c.Mount, string(header))
					if c.closeConnIfCurrent(conn) {
						c.notifyNetError(fmt.Errorf("unexpected ntrip authentication response: %q", firstLine(header)))
					}
					return
				}
				if c.getConn() != conn {
					_ = conn.Close()
					return
				}
				if connected.CompareAndSwap(false, true) {
					setReadDeadline(conn, 0)
					logPrintln("✅ntrip client auth ok! ", c.Mount, c.Username, maskSecret(c.Password))
					if callback := c.connectCallback(); callback != nil {
						callback(conn.LocalAddr().String(), c.Mount, conn)
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

func isNtripSuccessHeader(header []byte) bool {
	line := strings.ToUpper(firstLine(header))
	return strings.HasPrefix(line, "ICY 200 ") ||
		strings.HasPrefix(line, "HTTP/1.0 200 ") ||
		strings.HasPrefix(line, "HTTP/1.1 200 ")
}

func firstLine(data []byte) string {
	line := data
	if end := bytes.Index(line, []byte("\r\n")); end >= 0 {
		line = line[:end]
	} else if end := bytes.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	return strings.TrimSpace(string(line))
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

func createNtripAuthMsgV2(mountPoint, username, password, host string) string {
	head := fmt.Sprintf("GET /%s HTTP/1.1\r\n", mountPoint)
	head += fmt.Sprintf("Host: %s\r\n", host)
	head += "Ntrip-Version: Ntrip/2.0\r\n"
	head += "User-Agent: NTRIP nav-rtlogging-go-lib\r\n"
	if utf8.RuneCountInString(username) > 0 && utf8.RuneCountInString(password) > 0 {
		auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		head += fmt.Sprintf("Authorization: Basic %s\r\n", auth)
	}
	head += "Accept: */*\r\n\r\n"
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
	lineComplete := false
	if lineEnd := bytes.Index(line, []byte("\r\n")); lineEnd >= 0 {
		line = line[:lineEnd]
		lineComplete = true
	}
	if len(line) == 0 {
		return true
	}
	for _, b := range line {
		if b < 0x20 || b >= 0x7f {
			return false
		}
	}
	if colon := bytes.IndexByte(line, ':'); colon > 0 {
		for _, b := range line[:colon] {
			if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '-' {
				continue
			}
			return false
		}
		return true
	}
	if lineComplete {
		return false
	}
	prefix := strings.ToLower(string(line))
	for _, knownHeader := range []string{"server:", "date:", "ntrip-version:", "content-type:", "connection:"} {
		if strings.HasPrefix(knownHeader, prefix) {
			return true
		}
	}
	return false
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
