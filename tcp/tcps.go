package tcp

import (
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

// Server 封装结构
type Server struct {
	addr       string
	ln         net.Listener
	conn       net.Conn
	conns      map[net.Conn]struct{}
	mu         sync.Mutex
	onConnect  OnConnectFunc
	disConnect DisConnectFunc
	onData     OnDataFunc
	onSize     OnSizeFunc
	netError   NetErrorFunc
	done       chan struct{}
	stopOnce   sync.Once
}

// NewTcps 创建新 Server 实例
func NewTcps(port int) *Server {
	return &Server{
		addr:  fmt.Sprintf(":%d", port),
		conns: make(map[net.Conn]struct{}),
		done:  make(chan struct{}),
	}
}

// OnConnect 设置连接回调
func (s *Server) OnConnect(f OnConnectFunc) {
	s.onConnect = f
}

// DisConnect 设置断开回调
func (s *Server) DisConnect(f DisConnectFunc) {
	s.disConnect = f
}

// OnData 设置数据回调
func (s *Server) OnData(f OnDataFunc) {
	s.onData = f
}

// OnSize 设置数据大小回调
func (s *Server) OnSize(f OnSizeFunc) {
	s.onSize = f
}

// NetError 设置网络错误回调
func (s *Server) NetError(f NetErrorFunc) {
	s.netError = f
}

// Start 启动服务
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	fmt.Println("tcp server listening on", s.addr)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
				}
				fmt.Println("❌tcp server accept error:", err)
				continue
			}
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetKeepAlive(true)
				_ = tcpConn.SetKeepAlivePeriod(2 * time.Minute)
			}
			s.addConn(conn)
			if s.onConnect != nil {
				s.onConnect(conn)
			}
			go s.handleConn(conn)
		}
	}()
	return nil
}

func (s *Server) addConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = conn
	s.conns[conn] = struct{}{}
}

func (s *Server) removeConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
	if s.conn == conn {
		s.conn = nil
	}
}

func (s *Server) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		close(s.done)
		s.mu.Lock()
		if s.ln != nil {
			err = s.ln.Close()
			s.ln = nil
		}
		conns := make([]net.Conn, 0, len(s.conns))
		for conn := range s.conns {
			conns = append(conns, conn)
		}
		s.conns = make(map[net.Conn]struct{})
		s.conn = nil
		s.mu.Unlock()

		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return err
}

// handleConn 处理每个连接
func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.removeConn(conn)
		_ = conn.Close()
	}()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("❌tcp server read error: ", err)
			if err == io.EOF {
				fmt.Println("❌tcp server disconnect from:", conn.RemoteAddr())
			}
			if s.disConnect != nil {
				s.disConnect(conn)
			}
			break
		}
		bytes := buf[:n]
		if s.onData != nil {
			s.onData(conn, bytes)
		}
		if s.onSize != nil && len(bytes) > 0 {
			s.onSize(conn, len(bytes))
		}
	}
}
