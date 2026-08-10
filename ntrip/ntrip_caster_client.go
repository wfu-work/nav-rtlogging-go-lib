package ntrip

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// NtripCasterClient accepts NTRIP subscriber connections.
type NtripCasterClient struct {
	addr                   string
	ln                     net.Listener
	listen                 func(network, address string) (net.Listener, error)
	ntripMap               *SafeNtripMap
	casterServer           *NtripCasterServer
	onConnect              OnConnectFunc
	disConnect             OnDisConnectFunc
	onData                 OnDataFunc
	onSize                 OnSizeFunc
	onAuth                 OnAuthFunc
	onNetError             OnNetErrorFunc
	done                   chan struct{}
	conns                  map[net.Conn]string
	connectionsByIP        map[string]int
	mu                     sync.RWMutex
	callbackMu             sync.RWMutex
	tlsConfig              *tls.Config
	maxConnections         int
	maxConnectionsPerIP    int
	requireAuth            bool
	requireActiveSource    bool
	requireSourcetableAuth bool
	wg                     sync.WaitGroup
}

// NewNtripCasterClient creates the subscriber-facing side of an embedded caster.
func NewNtripCasterClient(port int) *NtripCasterClient {
	return NewNtripCasterClientOnAddress(DefaultNtripCasterBindAddress, port)
}

// NewNtripCasterClientOnAddress creates a subscriber listener bound to host.
func NewNtripCasterClientOnAddress(host string, port int) *NtripCasterClient {
	return &NtripCasterClient{
		addr:            net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		listen:          net.Listen,
		ntripMap:        NewSafeNtripMap(),
		done:            make(chan struct{}),
		conns:           make(map[net.Conn]string),
		connectionsByIP: make(map[string]int),
		maxConnections:  defaultNtripMaxConnections,
		requireAuth:     !isLoopbackBindAddress(host),
	}
}

// SetMaxConnectionsPerIP sets the maximum number of subscriber connections
// from one remote IP address. A value of zero disables the per-IP limit.
func (s *NtripCasterClient) SetMaxConnectionsPerIP(limit int) error {
	if limit < 0 {
		return errors.New("ntrip caster client max connections per IP cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster client max connections per IP while running")
	}
	s.maxConnectionsPerIP = limit
	return nil
}

// SetRequireActiveSource controls whether subscriber requests for a mount
// without an active source are rejected with 404. It is disabled by default
// so subscribers may connect before their source comes online.
func (s *NtripCasterClient) SetRequireActiveSource(required bool) {
	s.mu.Lock()
	s.requireActiveSource = required
	s.mu.Unlock()
}

// SetRequireSourcetableAuth controls whether GET / requires Basic
// authentication. Sourcetable access is public by default.
func (s *NtripCasterClient) SetRequireSourcetableAuth(required bool) {
	s.mu.Lock()
	s.requireSourcetableAuth = required
	s.mu.Unlock()
}

func (s *NtripCasterClient) setCasterServer(server *NtripCasterServer) {
	s.mu.Lock()
	s.casterServer = server
	s.mu.Unlock()
}

func (s *NtripCasterClient) clearCasterServer(server *NtripCasterServer) {
	s.mu.Lock()
	if s.casterServer == server {
		s.casterServer = nil
	}
	s.mu.Unlock()
}

func (s *NtripCasterClient) sourceSettings() (*NtripCasterServer, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.casterServer, s.requireActiveSource, s.requireSourcetableAuth
}

// SetTLSConfig enables TLS for incoming subscriber connections. It must be
// called before Start. A nil config keeps the listener on plain TCP.
func (s *NtripCasterClient) SetTLSConfig(config *tls.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster client TLS config while running")
	}
	if config == nil {
		s.tlsConfig = nil
		return nil
	}
	if len(config.Certificates) == 0 && config.GetCertificate == nil && config.GetConfigForClient == nil {
		return errors.New("ntrip caster client TLS config requires a server certificate")
	}
	s.tlsConfig = config.Clone()
	return nil
}

// SetMaxConnections sets the maximum number of pending and authenticated
// subscriber connections. A value of zero disables the limit.
func (s *NtripCasterClient) SetMaxConnections(limit int) error {
	if limit < 0 {
		return errors.New("ntrip caster client max connections cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster client max connections while running")
	}
	s.maxConnections = limit
	return nil
}

func (s *NtripCasterClient) OnConnect(f OnConnectFunc) {
	s.callbackMu.Lock()
	s.onConnect = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) DisConnect(f OnDisConnectFunc) {
	s.callbackMu.Lock()
	s.disConnect = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) OnData(f OnDataFunc) {
	s.callbackMu.Lock()
	s.onData = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) OnSize(f OnSizeFunc) {
	s.callbackMu.Lock()
	s.onSize = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) OnAuth(f OnAuthFunc) {
	s.callbackMu.Lock()
	s.onAuth = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) NetError(f OnNetErrorFunc) {
	s.callbackMu.Lock()
	s.onNetError = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterClient) callbacks() (OnConnectFunc, OnDisConnectFunc, OnDataFunc, OnSizeFunc, OnAuthFunc, OnNetErrorFunc) {
	s.callbackMu.RLock()
	defer s.callbackMu.RUnlock()
	return s.onConnect, s.disConnect, s.onData, s.onSize, s.onAuth, s.onNetError
}

// Addr returns the active listener address, or nil when stopped.
func (s *NtripCasterClient) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Start starts accepting subscriber connections. A stopped server may be started again.
func (s *NtripCasterClient) Start() error {
	_, _, _, _, onAuth, _ := s.callbacks()
	s.mu.Lock()
	if s.ln != nil {
		s.mu.Unlock()
		return errors.New("ntrip caster client listener already started")
	}
	if s.requireAuth && onAuth == nil {
		s.mu.Unlock()
		return errors.New("external ntrip caster client listener requires an authentication callback")
	}
	listen := s.listen
	if listen == nil {
		listen = net.Listen
	}
	ln, err := listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		_, _, _, _, _, onNetError := s.callbacks()
		if onNetError != nil {
			onNetError(err)
		}
		return err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig.Clone())
	}
	if s.done == nil || isClosed(s.done) {
		s.done = make(chan struct{})
	}
	s.ln = ln
	done := s.done
	s.wg.Add(1)
	s.mu.Unlock()
	logPrintln("✅ntrip caster client listening on", ln.Addr())

	go func() {
		defer s.wg.Done()
		s.acceptLoop(ln, done)
	}()
	return nil
}

func (s *NtripCasterClient) acceptLoop(ln net.Listener, done <-chan struct{}) {
	var retryDelay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if isClosed(done) || errors.Is(err, net.ErrClosed) {
				return
			}
			_, _, _, _, _, onNetError := s.callbacks()
			if onNetError != nil {
				onNetError(err)
			}
			if retryDelay == 0 {
				retryDelay = 5 * time.Millisecond
			} else {
				retryDelay *= 2
				if retryDelay > time.Second {
					retryDelay = time.Second
				}
			}
			timer := time.NewTimer(retryDelay)
			select {
			case <-done:
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		retryDelay = 0
		s.mu.Lock()
		if s.ln != ln {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		if s.maxConnections > 0 && len(s.conns) >= s.maxConnections {
			s.mu.Unlock()
			writeNtripRejection(conn, "HTTP/1.0 503 Service Unavailable\r\nConnection: close\r\n\r\n")
			_ = conn.Close()
			continue
		}
		ip := remoteIP(conn.RemoteAddr())
		if s.maxConnectionsPerIP > 0 && s.connectionsByIP[ip] >= s.maxConnectionsPerIP {
			s.mu.Unlock()
			writeNtripRejection(conn, "HTTP/1.0 429 Too Many Requests\r\nConnection: close\r\n\r\n")
			_ = conn.Close()
			continue
		}
		s.conns[conn] = ip
		s.connectionsByIP[ip]++
		s.wg.Add(1)
		s.mu.Unlock()
		go func(conn net.Conn) {
			defer s.wg.Done()
			s.handleConn(conn)
		}(conn)
	}
}

// Stop closes the listener and every authenticated or pending connection.
func (s *NtripCasterClient) Stop() error {
	s.mu.Lock()
	ln := s.ln
	s.ln = nil
	if s.done != nil && !isClosed(s.done) {
		close(s.done)
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.conns = make(map[net.Conn]string)
	s.connectionsByIP = make(map[string]int)
	s.mu.Unlock()

	s.ntripMap.CloseAll()
	var err error
	if ln != nil {
		err = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return err
}

// Shutdown stops the subscriber listener and waits for all accepted
// connections and background goroutines to exit.
func (s *NtripCasterClient) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := s.Stop()
	if waitErr := waitGroupContext(ctx, &s.wg); waitErr != nil {
		return waitErr
	}
	return err
}

func (s *NtripCasterClient) handleConn(conn net.Conn) {
	enableTCPKeepAlive(conn)
	key := conn.RemoteAddr().String()
	var bean *NtripChannelBean
	defer func() {
		s.mu.Lock()
		if ip, ok := s.conns[conn]; ok {
			delete(s.conns, conn)
			if s.connectionsByIP[ip] <= 1 {
				delete(s.connectionsByIP, ip)
			} else {
				s.connectionsByIP[ip]--
			}
		}
		s.mu.Unlock()
		removed := s.ntripMap.Delete(key)
		if removed != nil {
			_, onDisconnect, _, _, _, _ := s.callbacks()
			if onDisconnect != nil {
				onDisconnect(key, removed.mount)
			}
			removed.Close()
			logPrintf("ntrip caster client disconnected: %s - %s", key, removed.mount)
		} else {
			_ = conn.Close()
		}
	}()

	buf := make([]byte, defaultNtripReadBufferSize)
	authBuf := make([]byte, 0, 512)
	for {
		if bean == nil {
			setReadDeadline(conn, defaultNtripAuthTimeout)
		}
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logPrintf("ntrip caster client read error from %s: %v", key, err)
			}
			return
		}
		data := buf[:n]
		if bean == nil {
			authBuf = append(authBuf, data...)
			headerEnd := findNtripRequestHeaderEnd(authBuf)
			if headerEnd < 0 {
				if len(authBuf) > defaultNtripMaxHeaderSize {
					_ = WriteData(conn, []byte("HTTP/1.0 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n"))
					return
				}
				continue
			}
			bean = s.auth(conn, authBuf[:headerEnd])
			if bean == nil {
				return
			}
			setReadDeadline(conn, 0)
			s.ntripMap.Set(key, bean)
			onConnect, _, _, _, _, _ := s.callbacks()
			if onConnect != nil {
				onConnect(key, bean.mount, conn)
			}
			if payload := authBuf[headerEnd:]; len(payload) > 0 {
				s.processClientData(key, bean, conn, payload)
			}
			authBuf = nil
			continue
		}
		s.processClientData(key, bean, conn, data)
	}
}

func (s *NtripCasterClient) processClientData(key string, bean *NtripChannelBean, conn net.Conn, data []byte) {
	if len(data) == 0 {
		return
	}
	_, _, onData, onSize, _, _ := s.callbacks()
	if onData != nil {
		onData(key, bean.mount, cloneBytes(data), bean.extra)
	}
	if onSize != nil {
		onSize(key, bean.mount, conn, len(data))
	}
}

func (s *NtripCasterClient) auth(conn net.Conn, data []byte) *NtripChannelBean {
	request, err := parseNtripRequest(data)
	if err != nil || request.method != "GET" {
		_ = WriteData(conn, []byte(ntripAuthResponse(false)))
		return nil
	}
	server, requireActiveSource, requireSourcetableAuth := s.sourceSettings()
	if request.target == "" {
		if requireSourcetableAuth {
			if _, _, authorized := s.authorize(request); !authorized {
				_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
				return nil
			}
		}
		mounts := []string(nil)
		if server != nil {
			mounts = server.ntripMap.GetMountList()
		}
		_ = WriteData(conn, []byte(ntripSourcetableResponse(request, mounts)))
		return nil
	}
	if err := validateMount(request.target); err != nil {
		_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
		return nil
	}
	_, password, authorized := s.authorize(request)
	if !authorized {
		_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
		return nil
	}
	if requireActiveSource && (server == nil || !server.ntripMap.HasMount(request.target)) {
		_ = WriteData(conn, []byte(ntripNotFoundResponseForRequest(request)))
		return nil
	}
	if err := WriteData(conn, []byte(ntripAuthResponseForRequest(true, request))); err != nil {
		return nil
	}
	return NewNtripChannelBean(request.target, conn, password)
}

func (s *NtripCasterClient) authorize(request ntripRequest) (string, string, bool) {
	username, password, err := parseBasicAuthorization(request.headers["authorization"])
	if err != nil {
		return "", "", false
	}
	if request.target == "" && username == "" && password == "" {
		return username, password, false
	}
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := false
	if onAuth != nil {
		authorized = onAuth(request.target, username, password)
	} else if !s.requireAuth {
		authorized = request.target != "" && request.target == username && username == password
	}
	return username, password, authorized
}
