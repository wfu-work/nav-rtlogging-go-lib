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

// NtripCasterServer accepts NTRIP source connections.
type NtripCasterServer struct {
	addr                string
	ln                  net.Listener
	listen              func(network, address string) (net.Listener, error)
	ntripMap            *SafeNtripMap
	ntripCasterClient   *NtripCasterClient
	onConnect           OnConnectFunc
	disConnect          OnDisConnectFunc
	onData              OnDataFunc
	onSize              OnSizeFunc
	onAuth              OnAuthFunc
	onSpeed             OnSpeedFunc
	onNetError          OnNetErrorFunc
	done                chan struct{}
	conns               map[net.Conn]string
	connectionsByIP     map[string]int
	mu                  sync.RWMutex
	callbackMu          sync.RWMutex
	statsMu             sync.Mutex
	bytesByMount        map[string]int64
	tlsConfig           *tls.Config
	maxConnections      int
	maxConnectionsPerIP int
	requireAuth         bool
	wg                  sync.WaitGroup
}

// NewNtripCasterServer creates the source-facing side of an embedded caster.
func NewNtripCasterServer(port int) *NtripCasterServer {
	return NewNtripCasterServerOnAddress("127.0.0.1", port)
}

// NewNtripCasterServerOnAddress creates a source listener bound to host.
func NewNtripCasterServerOnAddress(host string, port int) *NtripCasterServer {
	return &NtripCasterServer{
		addr:            net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		listen:          net.Listen,
		ntripMap:        NewSafeNtripMap(),
		done:            make(chan struct{}),
		conns:           make(map[net.Conn]string),
		connectionsByIP: make(map[string]int),
		bytesByMount:    make(map[string]int64),
		maxConnections:  defaultNtripMaxConnections,
		requireAuth:     !isLoopbackBindAddress(host),
	}
}

// SetMaxConnectionsPerIP sets the maximum number of source connections from
// one remote IP address. A value of zero disables the per-IP limit.
func (s *NtripCasterServer) SetMaxConnectionsPerIP(limit int) error {
	if limit < 0 {
		return errors.New("ntrip caster source max connections per IP cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster source max connections per IP while running")
	}
	s.maxConnectionsPerIP = limit
	return nil
}

// SetTLSConfig enables TLS for incoming source connections. It must be called
// before Start. A nil config keeps the listener on plain TCP.
func (s *NtripCasterServer) SetTLSConfig(config *tls.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster source TLS config while running")
	}
	if config == nil {
		s.tlsConfig = nil
		return nil
	}
	if len(config.Certificates) == 0 && config.GetCertificate == nil && config.GetConfigForClient == nil {
		return errors.New("ntrip caster source TLS config requires a server certificate")
	}
	s.tlsConfig = config.Clone()
	return nil
}

// SetMaxConnections sets the maximum number of pending and authenticated
// source connections. A value of zero disables the limit.
func (s *NtripCasterServer) SetMaxConnections(limit int) error {
	if limit < 0 {
		return errors.New("ntrip caster source max connections cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change ntrip caster source max connections while running")
	}
	s.maxConnections = limit
	return nil
}

func (s *NtripCasterServer) OnConnect(f OnConnectFunc) {
	s.callbackMu.Lock()
	s.onConnect = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) DisConnect(f OnDisConnectFunc) {
	s.callbackMu.Lock()
	s.disConnect = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) OnData(f OnDataFunc) {
	s.callbackMu.Lock()
	s.onData = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) OnSize(f OnSizeFunc) {
	s.callbackMu.Lock()
	s.onSize = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) OnAuth(f OnAuthFunc) {
	s.callbackMu.Lock()
	s.onAuth = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) OnSpeed(f OnSpeedFunc) {
	s.callbackMu.Lock()
	s.onSpeed = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) NetError(f OnNetErrorFunc) {
	s.callbackMu.Lock()
	s.onNetError = f
	s.callbackMu.Unlock()
}

func (s *NtripCasterServer) callbacks() (OnConnectFunc, OnDisConnectFunc, OnDataFunc, OnSizeFunc, OnAuthFunc, OnNetErrorFunc) {
	s.callbackMu.RLock()
	defer s.callbackMu.RUnlock()
	return s.onConnect, s.disConnect, s.onData, s.onSize, s.onAuth, s.onNetError
}

func (s *NtripCasterServer) speedCallback() OnSpeedFunc {
	s.callbackMu.RLock()
	defer s.callbackMu.RUnlock()
	return s.onSpeed
}

func (s *NtripCasterServer) SetNtripCasterClient(client *NtripCasterClient) {
	s.mu.Lock()
	previous := s.ntripCasterClient
	s.ntripCasterClient = client
	s.mu.Unlock()
	if previous != nil && previous != client {
		previous.clearCasterServer(s)
	}
	if client != nil {
		client.setCasterServer(s)
	}
}

func (s *NtripCasterServer) casterClient() *NtripCasterClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ntripCasterClient
}

func (s *NtripCasterServer) GetNtripMap() *SafeNtripMap {
	return s.ntripMap
}

// Addr returns the active listener address, or nil when stopped.
func (s *NtripCasterServer) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Start starts accepting source connections. A stopped server may be started again.
func (s *NtripCasterServer) Start() error {
	_, _, _, _, onAuth, _ := s.callbacks()
	s.mu.Lock()
	if s.ln != nil {
		s.mu.Unlock()
		return errors.New("ntrip caster source listener already started")
	}
	if s.requireAuth && onAuth == nil {
		s.mu.Unlock()
		return errors.New("external ntrip caster source listener requires an authentication callback")
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
	s.wg.Add(2)
	s.mu.Unlock()
	logPrintln("✅ntrip caster server listening on", ln.Addr())

	go func() {
		defer s.wg.Done()
		s.acceptLoop(ln, done)
	}()
	go func() {
		defer s.wg.Done()
		s.speedLoop(done)
	}()
	return nil
}

func (s *NtripCasterServer) acceptLoop(ln net.Listener, done <-chan struct{}) {
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
		// Add while holding mu so Shutdown cannot begin waiting before this
		// accepted connection has been registered with the WaitGroup.
		s.wg.Add(1)
		s.mu.Unlock()
		go func(conn net.Conn) {
			defer s.wg.Done()
			s.handleConn(conn)
		}(conn)
	}
}

// Stop closes the listener and every authenticated or pending connection.
func (s *NtripCasterServer) Stop() error {
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
	s.statsMu.Lock()
	s.bytesByMount = make(map[string]int64)
	s.statsMu.Unlock()
	var err error
	if ln != nil {
		err = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return err
}

// Shutdown stops the source listener and waits for all accepted connections
// and background goroutines to exit.
func (s *NtripCasterServer) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := s.Stop()
	if waitErr := waitGroupContext(ctx, &s.wg); waitErr != nil {
		return waitErr
	}
	return err
}

func (s *NtripCasterServer) handleConn(conn net.Conn) {
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
			s.statsMu.Lock()
			delete(s.bytesByMount, removed.mount)
			s.statsMu.Unlock()
			_, onDisconnect, _, _, _, _ := s.callbacks()
			if onDisconnect != nil {
				onDisconnect(key, removed.mount)
			}
			logPrintf("ntrip caster source disconnected: %s - %s", key, removed.mount)
		}
		_ = conn.Close()
	}()

	buf := make([]byte, defaultNtripReadBufferSize)
	authBuf := make([]byte, 0, 512)
	for {
		if bean == nil {
			setReadDeadline(conn, defaultNtripAuthTimeout)
		} else {
			setReadDeadline(conn, defaultNtripSourceIdleTimeout)
		}
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logPrintf("ntrip caster source read error from %s: %v", key, err)
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
			bean = s.auth(conn, authBuf[:headerEnd], key)
			if bean == nil {
				return
			}
			setReadDeadline(conn, defaultNtripSourceIdleTimeout)
			onConnect, _, _, _, _, _ := s.callbacks()
			if onConnect != nil {
				onConnect(key, bean.mount, conn)
			}
			if payload := authBuf[headerEnd:]; len(payload) > 0 {
				s.processSourceData(key, bean, conn, payload)
			}
			authBuf = nil
			continue
		}
		s.processSourceData(key, bean, conn, data)
	}
}

func (s *NtripCasterServer) processSourceData(key string, bean *NtripChannelBean, conn net.Conn, data []byte) {
	if len(data) == 0 {
		return
	}
	if client := s.casterClient(); client != nil {
		client.ntripMap.BroadcastLossByMount(bean.mount, cloneBytes(data))
	}
	s.statsMu.Lock()
	s.bytesByMount[bean.mount] += int64(len(data))
	s.statsMu.Unlock()
	_, _, onData, onSize, _, _ := s.callbacks()
	if onData != nil {
		onData(key, bean.mount, cloneBytes(data), bean.extra)
	}
	if onSize != nil {
		onSize(key, bean.mount, conn, len(data))
	}
}

func (s *NtripCasterServer) speedLoop(done <-chan struct{}) {
	const interval = 15 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			speeds := s.takeMountSpeeds(interval)
			callback := s.speedCallback()
			if callback == nil {
				continue
			}
			for mount, speed := range speeds {
				callback(mount, speed)
			}
		}
	}
}

func (s *NtripCasterServer) takeMountSpeeds(interval time.Duration) map[string]int64 {
	seconds := int64(interval / time.Second)
	if seconds <= 0 {
		seconds = 1
	}
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	speeds := make(map[string]int64, len(s.bytesByMount))
	for mount, size := range s.bytesByMount {
		speeds[mount] = size / seconds
		s.bytesByMount[mount] = 0
	}
	return speeds
}

func (s *NtripCasterServer) auth(conn net.Conn, data []byte, key string) *NtripChannelBean {
	request, err := parseNtripRequest(data)
	if err != nil || (request.method != "SOURCE" && request.method != "POST") {
		_ = WriteData(conn, []byte(ntripAuthResponse(false)))
		return nil
	}
	if err := validateMount(request.target); err != nil {
		_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
		return nil
	}

	mount := request.target
	username := ""
	password := ""
	switch request.method {
	case "SOURCE":
		password = request.parts[1]
	case "POST":
		username, password, err = parseBasicAuthorization(request.headers["authorization"])
		if err != nil {
			_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
			return nil
		}
	}
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := false
	if onAuth != nil {
		authorized = onAuth(mount, username, password)
	} else if !s.requireAuth {
		authorized = mount == password
	}
	if !authorized {
		_ = WriteData(conn, []byte(ntripAuthResponseForRequest(false, request)))
		return nil
	}
	bean := &NtripChannelBean{mount: mount, conn: conn}
	if !s.ntripMap.SetIfMountAbsent(key, bean) {
		_ = WriteData(conn, []byte(ntripConflictResponseForRequest(request)))
		return nil
	}
	if err := WriteData(conn, []byte(ntripAuthResponseForRequest(true, request))); err != nil {
		s.ntripMap.Delete(key)
		return nil
	}
	return bean
}

func (s *NtripCasterServer) authNtripServer1(data string) (string, string, bool) {
	return s.authLegacySource(data)
}

func (s *NtripCasterServer) authNtripServer2(data string) (string, string, bool) {
	return s.authLegacySource(data)
}

func (s *NtripCasterServer) authLegacySource(data string) (string, string, bool) {
	request, err := parseNtripRequest([]byte(data))
	if err != nil || request.method != "SOURCE" {
		return "", "", false
	}
	if err := validateMount(request.target); err != nil {
		return request.target, "", false
	}
	password := request.parts[1]
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := false
	if onAuth != nil {
		authorized = onAuth(request.target, "", password)
	} else if !s.requireAuth {
		authorized = request.target == password
	}
	return request.target, password, authorized
}

func (s *NtripCasterServer) authNtripServer3(data string) (string, string, bool) {
	request, err := parseNtripRequest([]byte(data))
	if err != nil || request.method != "POST" {
		return "", "", false
	}
	if err := validateMount(request.target); err != nil {
		return request.target, "", false
	}
	username, password, err := parseBasicAuthorization(request.headers["authorization"])
	if err != nil {
		return request.target, "", false
	}
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := false
	if onAuth != nil {
		authorized = onAuth(request.target, username, password)
	} else if !s.requireAuth {
		authorized = request.target == password
	}
	return request.target, password, authorized
}
