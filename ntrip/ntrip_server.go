package ntrip

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// NtripServer represents an NTRIP source connection. Configure exported fields
// before Start or Connect; use Shutdown before changing them for a reconnect.
type NtripServer struct {
	Host           string
	Port           int
	Mount          string
	Username       string
	Password       string
	DialTimeout    time.Duration
	TLSConfig      *tls.Config
	UseNtripV2     bool
	dial           func(context.Context, string, int, time.Duration, *tls.Config) (net.Conn, error)
	Conn           net.Conn // Deprecated: use Connection or Write instead.
	onConnect      OnConnectFunc
	onDisConnect   OnDisConnectFunc
	onDataCallback OnDataFunc
	onNetError     OnNetErrorFunc
	connMu         sync.RWMutex
	startMu        sync.Mutex
	callbackMu     sync.RWMutex
	writeMu        sync.Mutex
	dialMu         sync.Mutex
	dialCancel     context.CancelFunc
	wg             sync.WaitGroup
}

// NewNtripServer creates a new NTRIP source connection.
func NewNtripServer(host string, port int, mount string, username string, password string) *NtripServer {
	return &NtripServer{
		Host:        host,
		Port:        port,
		Mount:       mount,
		Username:    username,
		Password:    password,
		DialTimeout: 5 * time.Second,
		dial:        dialNtrip,
	}
}

// OnConnect sets the callback invoked after source authentication succeeds.
func (s *NtripServer) OnConnect(f OnConnectFunc) {
	s.callbackMu.Lock()
	s.onConnect = f
	s.callbackMu.Unlock()
}

// DisConnect sets the callback invoked after an authenticated connection ends.
func (s *NtripServer) DisConnect(f OnDisConnectFunc) {
	s.callbackMu.Lock()
	s.onDisConnect = f
	s.callbackMu.Unlock()
}

// OnDataCallback sets the callback for payload received after authentication.
func (s *NtripServer) OnDataCallback(f OnDataFunc) {
	s.callbackMu.Lock()
	s.onDataCallback = f
	s.callbackMu.Unlock()
}

// OnNetErrorCallback sets the callback for dial, authentication, and read errors.
func (s *NtripServer) OnNetErrorCallback(f OnNetErrorFunc) {
	s.callbackMu.Lock()
	s.onNetError = f
	s.callbackMu.Unlock()
}

func (s *NtripServer) connectionCallbacks() (OnConnectFunc, OnDisConnectFunc, OnDataFunc, OnNetErrorFunc) {
	s.callbackMu.RLock()
	defer s.callbackMu.RUnlock()
	return s.onConnect, s.onDisConnect, s.onDataCallback, s.onNetError
}

func (s *NtripServer) notifyNetError(err error) {
	_, _, _, callback := s.connectionCallbacks()
	if callback != nil {
		callback(err)
	}
}

func (s *NtripServer) replaceConn(conn net.Conn) net.Conn {
	s.connMu.Lock()
	old := s.Conn
	s.Conn = conn
	s.connMu.Unlock()
	return old
}

func (s *NtripServer) clearConnIfCurrent(conn net.Conn) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.Conn != conn {
		return false
	}
	s.Conn = nil
	return true
}

// Connection returns the current source connection, including one that is
// still completing authentication.
func (s *NtripServer) Connection() net.Conn {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.Conn
}

// Start connects to the caster and starts authentication asynchronously.
// Successful return means the TCP connection and authentication request were
// established; OnConnect reports completion of NTRIP authentication.
func (s *NtripServer) Start() error {
	_, _, err := s.startConnectionContext(context.Background())
	return err
}

// Connect establishes the source connection and waits for NTRIP
// authentication to complete.
func (s *NtripServer) Connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	authResult, conn, err := s.startConnectionContext(ctx)
	if err != nil {
		return err
	}
	select {
	case err := <-authResult:
		return err
	case <-ctx.Done():
		if s.clearConnIfCurrent(conn) {
			_ = conn.Close()
		}
		return ctx.Err()
	}
}

func (s *NtripServer) startConnectionContext(parent context.Context) (<-chan error, net.Conn, error) {
	s.startMu.Lock()
	mount := s.Mount
	username := s.Username
	password := s.Password
	if err := validateMount(mount); err != nil {
		s.startMu.Unlock()
		s.notifyNetError(err)
		return nil, nil, err
	}

	address := ntripAddress(s.Host, s.Port)
	dial := s.dial
	if dial == nil {
		dial = dialNtrip
	}
	dialCtx, cancel := context.WithCancel(parent)
	s.dialMu.Lock()
	s.dialCancel = cancel
	s.dialMu.Unlock()
	conn, err := dial(dialCtx, s.Host, s.Port, s.DialTimeout, s.TLSConfig)
	s.dialMu.Lock()
	s.dialCancel = nil
	s.dialMu.Unlock()
	cancel()
	if err != nil {
		err = fmt.Errorf("dial ntrip caster %s: %w", address, err)
		s.startMu.Unlock()
		s.notifyNetError(err)
		return nil, nil, err
	}
	enableTCPKeepAlive(conn)
	if old := s.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}

	authMessage := createNtripServerAuthMsg(mount, password)
	if s.UseNtripV2 {
		authMessage = createNtripServerV2AuthMsg(mount, username, password, address)
	}
	if err := WriteData(conn, []byte(authMessage)); err != nil {
		s.clearConnIfCurrent(conn)
		_ = conn.Close()
		err = fmt.Errorf("send ntrip source authentication: %w", err)
		s.startMu.Unlock()
		s.notifyNetError(err)
		return nil, nil, err
	}
	setReadDeadline(conn, defaultNtripAuthTimeout)
	authResult := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handleConnWithAuth(conn, authResult, mount)
	}()
	s.startMu.Unlock()
	return authResult, conn, nil
}

// Stop closes the current source connection. It is safe to call repeatedly.
func (s *NtripServer) Stop() error {
	s.dialMu.Lock()
	cancel := s.dialCancel
	s.dialMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	conn := s.replaceConn(nil)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Shutdown stops the source connection and waits for its read goroutine.
func (s *NtripServer) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := s.Stop()
	if waitErr := waitGroupContext(ctx, &s.wg); waitErr != nil {
		return waitErr
	}
	return err
}

// Write sends source payload on the current connection.
func (s *NtripServer) Write(data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return WriteData(s.Connection(), data)
}

func (s *NtripServer) handleConn(conn net.Conn) {
	s.handleConnWithAuth(conn, nil, s.Mount)
}

func (s *NtripServer) handleConnWithAuth(conn net.Conn, authResult chan<- error, mount string) {
	connected := false
	var disconnectOnce sync.Once
	var authOnce sync.Once
	reportAuth := func(err error) {
		if authResult != nil {
			authOnce.Do(func() { authResult <- err })
		}
	}
	defer reportAuth(ErrNtripAuthenticationInterrupted)
	defer func() {
		wasCurrent := s.clearConnIfCurrent(conn)
		_ = conn.Close()
		if connected && wasCurrent {
			_, callback, _, _ := s.connectionCallbacks()
			disconnectOnce.Do(func() {
				if callback != nil {
					callback(conn.LocalAddr().String(), mount)
				}
			})
		}
	}()

	buf := make([]byte, defaultNtripReadBufferSize)
	responseBuf := make([]byte, 0, 256)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !connected && !errors.Is(err, net.ErrClosed) {
				authErr := fmt.Errorf("read ntrip source authentication response: %w", err)
				reportAuth(authErr)
				s.notifyNetError(authErr)
			} else if connected && !errors.Is(err, net.ErrClosed) && err != io.EOF {
				s.notifyNetError(fmt.Errorf("read ntrip source connection: %w", err))
			}
			return
		}
		data := buf[:n]
		if !connected {
			responseBuf = append(responseBuf, data...)
			headerEnd := findNtripResponseHeaderEnd(responseBuf)
			if headerEnd < 0 {
				if len(responseBuf) > defaultNtripMaxHeaderSize {
					authErr := fmt.Errorf("ntrip source authentication response exceeds %d bytes", defaultNtripMaxHeaderSize)
					reportAuth(authErr)
					s.notifyNetError(authErr)
					return
				}
				continue
			}
			header := responseBuf[:headerEnd]
			if !isNtripSuccessHeader(header) {
				authErr := fmt.Errorf("ntrip source authentication failed: %q", firstLine(header))
				reportAuth(authErr)
				s.notifyNetError(authErr)
				return
			}
			if s.Connection() != conn {
				reportAuth(ErrNtripAuthenticationInterrupted)
				return
			}
			connected = true
			reportAuth(nil)
			setReadDeadline(conn, 0)
			onConnect, _, onData, _ := s.connectionCallbacks()
			if onConnect != nil {
				onConnect(conn.LocalAddr().String(), mount, conn)
			}
			if payload := responseBuf[headerEnd:]; len(payload) > 0 && onData != nil {
				onData(conn.LocalAddr().String(), mount, cloneBytes(payload), "")
			}
			responseBuf = nil
			continue
		}

		_, _, onData, _ := s.connectionCallbacks()
		if onData != nil {
			onData(conn.LocalAddr().String(), mount, cloneBytes(data), "")
		}
	}
}

// createNtripServerAuthMsg creates a legacy NTRIP v1 SOURCE request.
func createNtripServerAuthMsg(mountPoint, password string) string {
	return fmt.Sprintf("SOURCE %s /%s\r\nSource-Agent: NTRIP NtripServerCMD/1.0\r\n\r\n", password, mountPoint)
}

func createNtripServerV2AuthMsg(mountPoint, username, password, host string) string {
	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return fmt.Sprintf(
		"POST /%s HTTP/1.1\r\nHost: %s\r\nNtrip-Version: Ntrip/2.0\r\nUser-Agent: NTRIP nav-rtlogging-go-lib\r\nAuthorization: Basic %s\r\nContent-Type: gnss/data\r\n\r\n",
		mountPoint,
		host,
		auth,
	)
}
