package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
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
	addr       string
	ln         net.Listener
	listen     func(network, address string) (net.Listener, error)
	conns      map[net.Conn]struct{}
	mu         sync.RWMutex
	callbackMu sync.RWMutex
	onConnect  OnConnectFunc
	disConnect DisConnectFunc
	onData     OnDataFunc
	onSize     OnSizeFunc
	netError   NetErrorFunc
	done       chan struct{}
}

// NewTcps creates a new TCP server.
func NewTcps(port int) *Server {
	return NewTcpsOnAddress("", port)
}

// NewTcpsOnAddress creates a TCP server bound to host.
func NewTcpsOnAddress(host string, port int) *Server {
	return &Server{
		addr:   net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		listen: net.Listen,
		conns:  make(map[net.Conn]struct{}),
		done:   make(chan struct{}),
	}
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
	s.mu.Unlock()

	go s.acceptLoop(ln, done)
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
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		onConnect, _, _, _, _ := s.callbacks()
		if onConnect != nil {
			onConnect(conn)
		}
		go s.handleConn(conn)
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
	s.conns = make(map[net.Conn]struct{})
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

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		_, wasActive := s.conns[conn]
		delete(s.conns, conn)
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
