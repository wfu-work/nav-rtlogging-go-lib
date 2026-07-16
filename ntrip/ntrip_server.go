package ntrip

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// NtripServer represents an NTRIP source connection.
type NtripServer struct {
	Host           string
	Port           int
	Mount          string
	Username       string
	Password       string
	DialTimeout    time.Duration
	TLSConfig      *tls.Config
	UseNtripV2     bool
	dial           func(string, int, time.Duration, *tls.Config) (net.Conn, error)
	Conn           net.Conn // Deprecated: use Connection or Write instead.
	onConnect      OnConnectFunc
	onDisConnect   OnDisConnectFunc
	onDataCallback OnDataFunc
	onNetError     OnNetErrorFunc
	connMu         sync.RWMutex
	startMu        sync.Mutex
	callbackMu     sync.RWMutex
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
	s.startMu.Lock()
	if err := validateMount(s.Mount); err != nil {
		s.startMu.Unlock()
		s.notifyNetError(err)
		return err
	}

	address := ntripAddress(s.Host, s.Port)
	dial := s.dial
	if dial == nil {
		dial = dialNtrip
	}
	conn, err := dial(s.Host, s.Port, s.DialTimeout, s.TLSConfig)
	if err != nil {
		err = fmt.Errorf("dial ntrip caster %s: %w", address, err)
		s.startMu.Unlock()
		s.notifyNetError(err)
		return err
	}
	enableTCPKeepAlive(conn)
	if old := s.replaceConn(conn); old != nil && old != conn {
		_ = old.Close()
	}

	authMessage := createNtripServerAuthMsg(s.Mount, s.Password)
	if s.UseNtripV2 {
		authMessage = createNtripServerV2AuthMsg(s.Mount, s.Username, s.Password, address)
	}
	if err := WriteData(conn, []byte(authMessage)); err != nil {
		s.clearConnIfCurrent(conn)
		_ = conn.Close()
		err = fmt.Errorf("send ntrip source authentication: %w", err)
		s.startMu.Unlock()
		s.notifyNetError(err)
		return err
	}
	setReadDeadline(conn, defaultNtripAuthTimeout)
	go s.handleConn(conn)
	s.startMu.Unlock()
	return nil
}

// Stop closes the current source connection. It is safe to call repeatedly.
func (s *NtripServer) Stop() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	conn := s.replaceConn(nil)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Write sends source payload on the current connection.
func (s *NtripServer) Write(data []byte) error {
	return WriteData(s.Connection(), data)
}

func (s *NtripServer) handleConn(conn net.Conn) {
	connected := false
	var disconnectOnce sync.Once
	defer func() {
		wasCurrent := s.clearConnIfCurrent(conn)
		_ = conn.Close()
		if connected && wasCurrent {
			_, callback, _, _ := s.connectionCallbacks()
			disconnectOnce.Do(func() {
				if callback != nil {
					callback(conn.LocalAddr().String(), s.Mount)
				}
			})
		}
	}()

	buf := make([]byte, defaultNtripReadBufferSize)
	responseBuf := make([]byte, 0, 256)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && err != io.EOF {
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
					s.notifyNetError(fmt.Errorf("ntrip source authentication response exceeds %d bytes", defaultNtripMaxHeaderSize))
					return
				}
				continue
			}
			header := responseBuf[:headerEnd]
			if !isNtripSuccessHeader(header) {
				s.notifyNetError(fmt.Errorf("ntrip source authentication failed: %q", firstLine(header)))
				return
			}
			if s.Connection() != conn {
				return
			}
			connected = true
			setReadDeadline(conn, 0)
			onConnect, _, onData, _ := s.connectionCallbacks()
			if onConnect != nil {
				onConnect(conn.LocalAddr().String(), s.Mount, conn)
			}
			if payload := responseBuf[headerEnd:]; len(payload) > 0 && onData != nil {
				onData(conn.LocalAddr().String(), s.Mount, cloneBytes(payload), "")
			}
			responseBuf = nil
			continue
		}

		_, _, onData, _ := s.connectionCallbacks()
		if onData != nil {
			onData(conn.LocalAddr().String(), s.Mount, cloneBytes(data), "")
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
