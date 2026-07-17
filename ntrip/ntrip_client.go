package ntrip

import (
	"bytes"
	"context"
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

// NtripClient represents an NTRIP client. Configure exported connection fields
// before Start or Connect; use Shutdown before changing them for a reconnect.
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
	RetryInitial       time.Duration
	RetryMax           time.Duration
	TLSConfig          *tls.Config
	UseNtripV2         bool
	dial               func(context.Context, string, int, time.Duration, *tls.Config) (net.Conn, error)
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
	dialMu             sync.Mutex
	dialCancel         context.CancelFunc
	wg                 sync.WaitGroup
}

const (
	defaultNtripRetryInitial = 5 * time.Second
	defaultNtripRetryMax     = time.Minute
)

// ErrNtripAuthenticationInterrupted reports that a connection was replaced or
// stopped before authentication completed.
var ErrNtripAuthenticationInterrupted = errors.New("ntrip authentication interrupted")

type ntripClientRuntime struct {
	host        string
	port        int
	mount       string
	username    string
	password    string
	isGGA       bool
	ggaTime     int
	longitude   float64
	latitude    float64
	altitude    float64
	extra       string
	dialTimeout time.Duration
	tlsConfig   *tls.Config
	useV2       bool
}

func (c *NtripClient) runtimeConfig() ntripClientRuntime {
	return ntripClientRuntime{
		host: c.Host, port: c.Port, mount: c.Mount,
		username: c.Username, password: c.Password,
		isGGA: c.IsGga, ggaTime: c.GgaTime,
		longitude: c.Longitude, latitude: c.Latitude, altitude: c.Altitude,
		extra: c.Extra, dialTimeout: c.DialTimeout,
		tlsConfig: c.TLSConfig, useV2: c.UseNtripV2,
	}
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
		Host:         "127.0.0.1",
		Port:         9095,
		Mount:        mount,
		Username:     mount,
		Password:     mount,
		IsGga:        false,
		GgaTime:      5,
		DialTimeout:  5 * time.Second,
		RetryInitial: defaultNtripRetryInitial,
		RetryMax:     defaultNtripRetryMax,
		dial:         dialNtrip,
		quit:         make(chan struct{}),
	}
}

// NewNtripClient creates a new NtripClient.
func NewNtripClient(host string, port int, mount string, username string, password string) *NtripClient {
	return &NtripClient{
		Host:         host,
		Port:         port,
		Mount:        mount,
		Username:     username,
		Password:     password,
		IsGga:        false,
		GgaTime:      5,
		DialTimeout:  5 * time.Second,
		RetryInitial: defaultNtripRetryInitial,
		RetryMax:     defaultNtripRetryMax,
		dial:         dialNtrip,
		quit:         make(chan struct{}),
	}
}

// NewNtripClientExtra creates a new NtripClient.
func NewNtripClientExtra(host string, port int, mount string, username string, password string, extra string) *NtripClient {
	return &NtripClient{
		Host:         host,
		Port:         port,
		Mount:        mount,
		Username:     username,
		Password:     password,
		IsGga:        false,
		GgaTime:      5,
		Extra:        extra,
		DialTimeout:  5 * time.Second,
		RetryInitial: defaultNtripRetryInitial,
		RetryMax:     defaultNtripRetryMax,
		dial:         dialNtrip,
		quit:         make(chan struct{}),
	}
}

// NewNtripClientGgaExtra creates a new NtripClient.
func NewNtripClientGgaExtra(host string, port int, mount string, username string, password string, latitude float64, longitude float64, altitude float64, extra string) *NtripClient {
	return &NtripClient{
		Host:         host,
		Port:         port,
		Mount:        mount,
		Username:     username,
		Password:     password,
		IsGga:        true,
		GgaTime:      1,
		Latitude:     latitude,
		Longitude:    longitude,
		Altitude:     altitude,
		Extra:        extra,
		DialTimeout:  5 * time.Second,
		RetryInitial: defaultNtripRetryInitial,
		RetryMax:     defaultNtripRetryMax,
		dial:         dialNtrip,
		quit:         make(chan struct{}),
	}
}

func (c *NtripClient) Start() error {
	_, _, err := c.startConnectionContext(context.Background())
	return err
}

// Connect establishes a connection and waits until NTRIP authentication
// succeeds, the context is canceled, or authentication fails.
func (c *NtripClient) Connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	authResult, conn, err := c.startConnectionContext(ctx)
	if err != nil {
		return err
	}
	select {
	case err := <-authResult:
		return err
	case <-ctx.Done():
		c.closeConnIfCurrent(conn)
		return ctx.Err()
	}
}

func (c *NtripClient) startConnectionContext(parent context.Context) (<-chan error, net.Conn, error) {
	c.startMu.Lock()
	config := c.runtimeConfig()
	if err := validateMount(config.mount); err != nil {
		c.startMu.Unlock()
		c.notifyNetError(err)
		return nil, nil, err
	}
	if config.isGGA {
		if err := validateGGAInput(config.latitude, config.longitude, config.altitude); err != nil {
			c.startMu.Unlock()
			c.notifyNetError(err)
			return nil, nil, err
		}
	}
	quit := c.beginRun()
	dial := c.dial
	if dial == nil {
		dial = dialNtrip
	}
	dialCtx, cancel := context.WithCancel(parent)
	c.dialMu.Lock()
	c.dialCancel = cancel
	c.dialMu.Unlock()
	conn, err := dial(dialCtx, config.host, config.port, config.dialTimeout, config.tlsConfig)
	c.dialMu.Lock()
	c.dialCancel = nil
	c.dialMu.Unlock()
	cancel()
	if err != nil {
		c.startMu.Unlock()
		logPrintln("❌ntrip client start error: ", err, "exit!")
		c.notifyNetError(err)
		return nil, nil, err
	}
	enableTCPKeepAlive(conn)
	if old := c.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}
	authMsg := createNtripAuthMsg(config.mount, config.username, config.password)
	if config.useV2 {
		authMsg = createNtripAuthMsgV2(config.mount, config.username, config.password, ntripAddress(config.host, config.port))
	}
	if err = WriteData(conn, []byte(authMsg)); err != nil {
		logPrintln("❌ntrip client write error: ", err, "exit!")
		c.closeConnIfCurrent(conn)
		c.startMu.Unlock()
		c.notifyNetError(err)
		return nil, nil, err
	}
	logPrintln("✅ntrip client send auth msg for mount: ", config.mount)
	authResult := make(chan error, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.handleConnWithAuth(conn, quit, authResult, config)
	}()
	c.startMu.Unlock()
	return authResult, conn, nil
}

func (c *NtripClient) Stop() {
	c.dialMu.Lock()
	cancel := c.dialCancel
	c.dialMu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.startMu.Lock()
	safeClose(c.currentRun())
	c.closeConn()
	c.startMu.Unlock()
}

// Shutdown stops the client and waits for its connection, idle monitor, and
// GGA goroutines to exit.
func (c *NtripClient) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.Stop()
	return waitGroupContext(ctx, &c.wg)
}

func (c *NtripClient) Retry() {
	quit := c.currentRun()
	if isClosed(quit) {
		return
	}
	if !c.retrying.CompareAndSwap(false, true) {
		return
	}
	config := c.runtimeConfig()
	retryInitial := c.RetryInitial
	retryMax := c.RetryMax
	logPrintf("ntrip client %s-%d-%s will retry connection", config.host, config.port, config.mount)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer c.retrying.Store(false)
		delay := retryInitial
		if delay <= 0 {
			delay = defaultNtripRetryInitial
		}
		maxDelay := retryMax
		if maxDelay <= 0 {
			maxDelay = defaultNtripRetryMax
		}
		if maxDelay < delay {
			maxDelay = delay
		}
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
				authResult, _, err := c.startConnectionContext(context.Background())
				if err == nil {
					select {
					case <-quit:
						return
					case authErr := <-authResult:
						if authErr == nil {
							return
						}
					}
				}
				if delay >= maxDelay/2 {
					delay = maxDelay
				} else {
					delay *= 2
				}
				logPrintf("ntrip client %s-%d-%s connection failed; retrying in %s", config.host, config.port, config.mount, delay)
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
	c.handleConnWithAuth(conn, quit, nil, c.runtimeConfig())
}

func (c *NtripClient) handleConnWithAuth(conn net.Conn, quit <-chan struct{}, authResult chan<- error, config ntripClientRuntime) {
	connDone := make(chan struct{})
	defer close(connDone)
	var authOnce sync.Once
	reportAuth := func(err error) {
		if authResult == nil {
			return
		}
		authOnce.Do(func() { authResult <- err })
	}
	defer reportAuth(ErrNtripAuthenticationInterrupted)

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
				callback(conn.LocalAddr().String(), config.mount)
			}
		})
	}
	startGGA := func() {
		if !config.isGGA {
			return
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			interval := time.Duration(config.ggaTime) * time.Second
			if interval <= 0 {
				interval = time.Second
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-quit:
					logPrintln("🛑GGA发送停止:", config.mount)
					return
				case <-connDone:
					logPrintln("🛑GGA发送停止:", config.mount)
					return
				case <-ticker.C:
					gga := GenerateGGA(config.latitude, config.longitude, config.altitude)
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
			callback(conn.LocalAddr().String(), config.mount, cloneBytes(data), config.extra)
		}
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				logPrintln("🛑ntrip client read time out quit signal received:", config.mount)
				return
			case <-connDone:
				return
			case <-ticker.C:
				last := time.Unix(0, lastDataTime.Load())
				if time.Since(last) > defaultNtripClientIdleTimeout {
					logPrintln("⏱️超过60秒未收到数据，断开连接:", config.mount)
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
			logPrintln("🛑ntrip client quit signal received:", config.mount)
			if c.closeConnIfCurrent(conn) {
				notifyDisconnect()
			}
			reportAuth(ErrNtripAuthenticationInterrupted)
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
						authErr := fmt.Errorf("read ntrip authentication response: %w", io.EOF)
						reportAuth(authErr)
						c.notifyNetError(authErr)
					}
					return
				}
				logPrintln("❌ntrip client error reading:", err)
				if errors.Is(err, net.ErrClosed) {
					logPrintf("ℹ️连接已关闭（忽略重复错误）: %s\n", config.mount)
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
					authErr := fmt.Errorf("read ntrip authentication response: %w", err)
					reportAuth(authErr)
					c.notifyNetError(authErr)
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
					logPrintln("❌ntrip client auth failed! ", config.mount, config.username, maskSecret(config.password))
					authErr := fmt.Errorf("ntrip authentication failed for mount %q", config.mount)
					reportAuth(authErr)
					if c.closeConnIfCurrent(conn) {
						c.notifyNetError(authErr)
					}
					return
				}
				headerEnd := findNtripResponseHeaderEnd(responseBuf)
				if headerEnd < 0 {
					if len(responseBuf) > defaultNtripMaxHeaderSize {
						logPrintln("❌ntrip client auth response too large:", config.mount)
						authErr := fmt.Errorf("ntrip authentication response exceeds %d bytes", defaultNtripMaxHeaderSize)
						reportAuth(authErr)
						if c.closeConnIfCurrent(conn) {
							c.notifyNetError(authErr)
						}
						return
					}
					continue
				}
				header := responseBuf[:headerEnd]
				payload := responseBuf[headerEnd:]
				if !isNtripSuccessHeader(header) {
					logPrintln("❌ntrip client auth unexpected response:", config.mount, string(header))
					authErr := fmt.Errorf("unexpected ntrip authentication response: %q", firstLine(header))
					reportAuth(authErr)
					if c.closeConnIfCurrent(conn) {
						c.notifyNetError(authErr)
					}
					return
				}
				if c.getConn() != conn {
					reportAuth(ErrNtripAuthenticationInterrupted)
					_ = conn.Close()
					return
				}
				if connected.CompareAndSwap(false, true) {
					reportAuth(nil)
					setReadDeadline(conn, 0)
					logPrintln("✅ntrip client auth ok! ", config.mount, config.username, maskSecret(config.password))
					if callback := c.connectCallback(); callback != nil {
						callback(conn.LocalAddr().String(), config.mount, conn)
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
