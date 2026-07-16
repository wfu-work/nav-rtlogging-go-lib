package ntrip

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// NtripCasterServer accepts NTRIP source connections.
type NtripCasterServer struct {
	addr              string
	ln                net.Listener
	listen            func(network, address string) (net.Listener, error)
	ntripMap          *SafeNtripMap
	ntripCasterClient *NtripCasterClient
	onConnect         OnConnectFunc
	disConnect        OnDisConnectFunc
	onData            OnDataFunc
	onSize            OnSizeFunc
	onAuth            OnAuthFunc
	onSpeed           OnSpeedFunc
	onNetError        OnNetErrorFunc
	done              chan struct{}
	conns             map[net.Conn]struct{}
	mu                sync.RWMutex
	callbackMu        sync.RWMutex
	statsMu           sync.Mutex
	bytesByMount      map[string]int64
}

// NewNtripCasterServer creates the source-facing side of an embedded caster.
func NewNtripCasterServer(port int) *NtripCasterServer {
	return NewNtripCasterServerOnAddress("127.0.0.1", port)
}

// NewNtripCasterServerOnAddress creates a source listener bound to host.
func NewNtripCasterServerOnAddress(host string, port int) *NtripCasterServer {
	return &NtripCasterServer{
		addr:         net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		listen:       net.Listen,
		ntripMap:     NewSafeNtripMap(),
		done:         make(chan struct{}),
		conns:        make(map[net.Conn]struct{}),
		bytesByMount: make(map[string]int64),
	}
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
	s.ntripCasterClient = client
	s.mu.Unlock()
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
	s.mu.Lock()
	if s.ln != nil {
		s.mu.Unlock()
		return errors.New("ntrip caster source listener already started")
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
	if s.done == nil || isClosed(s.done) {
		s.done = make(chan struct{})
	}
	s.ln = ln
	done := s.done
	s.mu.Unlock()
	logPrintln("✅ntrip caster server listening on", ln.Addr())

	go s.acceptLoop(ln, done)
	go s.speedLoop(done)
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
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(conn)
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
	s.conns = make(map[net.Conn]struct{})
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

func (s *NtripCasterServer) handleConn(conn net.Conn) {
	enableTCPKeepAlive(conn)
	key := conn.RemoteAddr().String()
	var bean *NtripChannelBean
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
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
			bean = s.auth(conn, authBuf[:headerEnd])
			if bean == nil {
				return
			}
			setReadDeadline(conn, defaultNtripSourceIdleTimeout)
			s.ntripMap.Set(key, bean)
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
		client.ntripMap.ForEachByMount(bean.mount, func(subscriber *NtripChannelBean) {
			if subscriber != nil && subscriber.conn != nil {
				subscriber.SendLoss(data)
			}
		})
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

func (s *NtripCasterServer) auth(conn net.Conn, data []byte) *NtripChannelBean {
	request, err := parseNtripRequest(data)
	if err != nil || (request.method != "SOURCE" && request.method != "POST") {
		_ = WriteData(conn, []byte(ntripAuthResponse(false)))
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
	authorized := mount == password
	if onAuth != nil {
		authorized = onAuth(mount, username, password)
	}
	if err := WriteData(conn, []byte(ntripAuthResponseForRequest(authorized, request))); err != nil || !authorized {
		return nil
	}
	return &NtripChannelBean{mount: mount, conn: conn}
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
	password := request.parts[1]
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := request.target == password
	if onAuth != nil {
		authorized = onAuth(request.target, "", password)
	}
	return request.target, password, authorized
}

func (s *NtripCasterServer) authNtripServer3(data string) (string, string, bool) {
	request, err := parseNtripRequest([]byte(data))
	if err != nil || request.method != "POST" {
		return "", "", false
	}
	username, password, err := parseBasicAuthorization(request.headers["authorization"])
	if err != nil {
		return request.target, "", false
	}
	_, _, _, _, onAuth, _ := s.callbacks()
	authorized := request.target == password
	if onAuth != nil {
		authorized = onAuth(request.target, username, password)
	}
	return request.target, password, authorized
}
