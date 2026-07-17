package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type OnConnectFunc func(conn net.Conn)
type DisConnectFunc func(conn net.Conn)
type NetErrorFunc func(err error)
type OnDataFunc func(conn net.Conn, data []byte)
type OnSizeFunc func(conn net.Conn, size int)

// Server accepts TCP connections and dispatches connection callbacks.
type Server struct {
	addr                string
	ln                  net.Listener
	listen              func(network, address string) (net.Listener, error)
	conns               map[net.Conn]string
	connectionsByIP     map[string]int
	mu                  sync.RWMutex
	callbackMu          sync.RWMutex
	onConnect           OnConnectFunc
	disConnect          DisConnectFunc
	onData              OnDataFunc
	onSize              OnSizeFunc
	netError            NetErrorFunc
	done                chan struct{}
	maxConnections      int
	maxConnectionsPerIP int
	wg                  sync.WaitGroup
}

// NewTcps creates a new TCP server.
func NewTcps(port int) *Server {
	return NewTcpsOnAddress("", port)
}

// NewTcpsOnAddress creates a TCP server bound to host.
func NewTcpsOnAddress(host string, port int) *Server {
	return &Server{
		addr:            net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		listen:          net.Listen,
		conns:           make(map[net.Conn]string),
		connectionsByIP: make(map[string]int),
		done:            make(chan struct{}),
		maxConnections:  1024,
	}
}

// SetMaxConnectionsPerIP sets the maximum number of accepted connections
// from one remote IP address. A value of zero disables the per-IP limit.
func (s *Server) SetMaxConnectionsPerIP(limit int) error {
	if limit < 0 {
		return errors.New("tcp server max connections per IP cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change tcp server max connections per IP while running")
	}
	s.maxConnectionsPerIP = limit
	return nil
}

// SetMaxConnections sets the maximum number of accepted connections. A value
// of zero disables the limit. It must be called before Start.
func (s *Server) SetMaxConnections(limit int) error {
	if limit < 0 {
		return errors.New("tcp server max connections cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("cannot change tcp server max connections while running")
	}
	s.maxConnections = limit
	return nil
}

func (s *Server) OnConnect(f OnConnectFunc) {
	s.callbackMu.Lock()
	s.onConnect = f
	s.callbackMu.Unlock()
}

func (s *Server) DisConnect(f DisConnectFunc) {
	s.callbackMu.Lock()
	s.disConnect = f
	s.callbackMu.Unlock()
}

func (s *Server) OnData(f OnDataFunc) {
	s.callbackMu.Lock()
	s.onData = f
	s.callbackMu.Unlock()
}

func (s *Server) OnSize(f OnSizeFunc) {
	s.callbackMu.Lock()
	s.onSize = f
	s.callbackMu.Unlock()
}

func (s *Server) NetError(f NetErrorFunc) {
	s.callbackMu.Lock()
	s.netError = f
	s.callbackMu.Unlock()
}

func (s *Server) callbacks() (OnConnectFunc, DisConnectFunc, OnDataFunc, OnSizeFunc, NetErrorFunc) {
	s.callbackMu.RLock()
	defer s.callbackMu.RUnlock()
	return s.onConnect, s.disConnect, s.onData, s.onSize, s.netError
}

// Addr returns the active listener address, or nil when stopped.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Start starts accepting connections. A stopped server may be started again.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.ln != nil {
		s.mu.Unlock()
		return errors.New("tcp server already started")
	}
	listen := s.listen
	if listen == nil {
		listen = net.Listen
	}
	ln, err := listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if s.done == nil || channelClosed(s.done) {
		s.done = make(chan struct{})
	}
	s.ln = ln
	done := s.done
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		s.acceptLoop(ln, done)
	}()
	return nil
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *Server) acceptLoop(ln net.Listener, done <-chan struct{}) {
	var retryDelay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if channelClosed(done) || errors.Is(err, net.ErrClosed) {
				return
			}
			_, _, _, _, onError := s.callbacks()
			if onError != nil {
				onError(err)
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
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(2 * time.Minute)
		}
		s.mu.Lock()
		if s.ln != ln {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		if s.maxConnections > 0 && len(s.conns) >= s.maxConnections {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		ip := remoteIP(conn.RemoteAddr())
		if s.maxConnectionsPerIP > 0 && s.connectionsByIP[ip] >= s.maxConnectionsPerIP {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns[conn] = ip
		s.connectionsByIP[ip]++
		s.wg.Add(1)
		s.mu.Unlock()
		go func(conn net.Conn) {
			defer s.wg.Done()
			onConnect, _, _, _, _ := s.callbacks()
			if onConnect != nil {
				onConnect(conn)
			}
			s.handleConn(conn)
		}(conn)
	}
}

// Stop closes the listener and every accepted connection.
func (s *Server) Stop() error {
	s.mu.Lock()
	ln := s.ln
	s.ln = nil
	if s.done != nil && !channelClosed(s.done) {
		close(s.done)
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.conns = make(map[net.Conn]string)
	s.connectionsByIP = make(map[string]int)
	s.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return err
}

// Shutdown stops the listener and waits for accepted connections and the
// accept loop to exit.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := s.Stop()
	if waitErr := waitGroupContext(ctx, &s.wg); waitErr != nil {
		return waitErr
	}
	return err
}

func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		ip, wasActive := s.conns[conn]
		if wasActive {
			delete(s.conns, conn)
			if s.connectionsByIP[ip] <= 1 {
				delete(s.connectionsByIP, ip)
			} else {
				s.connectionsByIP[ip]--
			}
		}
		s.mu.Unlock()
		_ = conn.Close()
		if wasActive {
			_, onDisconnect, _, _, _ := s.callbacks()
			if onDisconnect != nil {
				onDisconnect(conn)
			}
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				_, _, _, _, onError := s.callbacks()
				if onError != nil {
					onError(err)
				}
			}
			return
		}
		_, _, onData, onSize, _ := s.callbacks()
		if onData != nil {
			data := append([]byte(nil), buf[:n]...)
			onData(conn, data)
		}
		if onSize != nil && n > 0 {
			onSize(conn, n)
		}
	}
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		if ip := tcpAddr.IP.String(); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err == nil && host != "" {
		return strings.Trim(host, "[]")
	}
	if value := strings.TrimSpace(addr.String()); value != "" {
		return value
	}
	return "unknown"
}
