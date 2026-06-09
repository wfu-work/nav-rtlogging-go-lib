package ntrip

import (
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

// NtripCasterClient 封装结构
type NtripCasterClient struct {
	addr       string
	ln         net.Listener
	ntripMap   *SafeNtripMap
	onConnect  OnConnectFunc
	disConnect OnDisConnectFunc
	onData     OnDataFunc
	onSize     OnSizeFunc
	onAuth     OnAuthFunc
	onNetError OnNetErrorFunc
	done       chan struct{}
	stopOnce   sync.Once
	mu         sync.Mutex
}

// NewNtripCasterClient 创建新 Server 实例
func NewNtripCasterClient(port int) *NtripCasterClient {
	return &NtripCasterClient{
		addr:     fmt.Sprintf(":%d", port),
		ntripMap: NewSafeNtripMap(),
		done:     make(chan struct{}),
	}
}

// OnConnect 设置连接回调
func (s *NtripCasterClient) OnConnect(f OnConnectFunc) {
	s.onConnect = f
}

// DisConnect 设置断开回调
func (s *NtripCasterClient) DisConnect(f OnDisConnectFunc) {
	s.disConnect = f
}

// OnData 设置数据回调
func (s *NtripCasterClient) OnData(f OnDataFunc) {
	s.onData = f
}

// OnSize 设置数据大小回调
func (s *NtripCasterClient) OnSize(f OnSizeFunc) {
	s.onSize = f
}

// OnAuth 设置认证回调
func (s *NtripCasterClient) OnAuth(f OnAuthFunc) {
	s.onAuth = f
}

// NetError 设置网络错误回调
func (s *NtripCasterClient) NetError(f OnNetErrorFunc) {
	s.onNetError = f
}

// Start 启动服务
func (s *NtripCasterClient) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Println("❌ntrip caster client net error:", err)
		if s.onNetError != nil {
			s.onNetError(err)
		}
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	log.Println("✅ntrip caster client listening on", s.addr)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
				}
				log.Println("❌ntrip caster client accept error:", err)
				continue
			}
			go s.handleConn(conn)
		}
	}()
	return nil
}

func (s *NtripCasterClient) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		close(s.done)
		s.ntripMap.CloseAll()
		s.mu.Lock()
		if s.ln != nil {
			err = s.ln.Close()
			s.ln = nil
		}
		s.mu.Unlock()
	})
	return err
}

// handleConn 处理每个连接
func (s *NtripCasterClient) handleConn(conn net.Conn) {
	enableTCPKeepAlive(conn)
	key := conn.RemoteAddr().String()
	defer func() {
		ntripChannelVo := s.ntripMap.Delete(key)
		if ntripChannelVo != nil {
			if s.disConnect != nil {
				s.disConnect(key, ntripChannelVo.mount)
			}
			ntripChannelVo.Close()
			log.Printf("❌ntrip caster client disconnect from: %s - %s", conn.RemoteAddr(), ntripChannelVo.mount)
			return
		}
		_ = conn.Close()
	}()

	authenticated := false
	buf := make([]byte, defaultNtripReadBufferSize)
	for {
		if !authenticated {
			setReadDeadline(conn, defaultNtripAuthTimeout)
		}
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("❌ntrip caster client read error: ", err)
			break
		}
		bytes := buf[:n]
		ntripChannelBean := s.ntripMap.Get(key)
		if ntripChannelBean == nil {
			ntripChannelBean = s.auth(conn, bytes)
			if ntripChannelBean != nil {
				authenticated = true
				setReadDeadline(conn, 0)
				s.ntripMap.Set(key, ntripChannelBean)
				if s.onConnect != nil {
					s.onConnect(key, ntripChannelBean.mount, conn)
				}
			}
		} else {
			if s.onData != nil {
				s.onData(key, ntripChannelBean.mount, bytes, ntripChannelBean.extra)
			}
		}
		if ntripChannelBean != nil && s.onSize != nil && len(bytes) > 0 {
			s.onSize(key, ntripChannelBean.mount, conn, len(bytes))
		}
	}
}

func (s *NtripCasterClient) auth(conn net.Conn, bytes []byte) *NtripChannelBean {
	dataStr := string(bytes)
	if strings.HasPrefix(dataStr, "GET") {
		log.Println("✅ntrip caster client auth request received from: ", conn.RemoteAddr().String())
		var authTag = false
		splits := strings.Split(dataStr, "\r\n")
		if len(splits) < 3 {
			log.Println("auth data invalid from: ", conn.RemoteAddr().String())
			_ = WriteData(conn, []byte("HTTP/1.0 401 Unauthorized\r\n"))
			_ = conn.Close()
			return nil
		}
		requestParts := strings.Fields(splits[0])
		if len(requestParts) < 2 {
			log.Println("auth data invalid from: ", conn.RemoteAddr().String())
			_ = WriteData(conn, []byte("HTTP/1.0 401 Unauthorized\r\n"))
			_ = conn.Close()
			return nil
		}
		mount := strings.ReplaceAll(requestParts[1], "/", "")
		authSplits := strings.Fields(splits[2])
		if len(authSplits) < 3 {
			log.Println("auth data invalid from: ", conn.RemoteAddr().String())
			_ = WriteData(conn, []byte("HTTP/1.0 401 Unauthorized\r\n"))
			_ = conn.Close()
			return nil
		}
		auth := authSplits[2]
		var username, password string
		if strings.TrimSpace(auth) != "" {
			authBytes, err := base64.StdEncoding.DecodeString(auth)
			if err != nil {
				log.Println("❌ntrip caster client error decoding base64:", err)
				_ = WriteData(conn, []byte("HTTP/1.0 401 Unauthorized\r\n"))
				_ = conn.Close()
				return nil
			}
			authStr := string(authBytes)
			split := strings.Split(authStr, ":")
			if len(split) >= 2 {
				username = split[0]
				password = split[1]
			}
		}
		if mount == username && username == password {
			authTag = true
		} else {
			if s.onAuth != nil {
				authTag = s.onAuth(mount, username, password)
			}
		}
		var result string
		if authTag {
			result = "ICY 200 OK\r\nServer: Trimble-iGate/1.0\r\nDate:" + NowNtripDate() + "\r\n\r\n"
		} else {
			result = "HTTP/1.0 401 Unauthorized\r\n"
		}
		log.Println("✅ntrip caster client auth result: ", mount, username, maskSecret(password), result)
		err := WriteData(conn, []byte(result))
		if err != nil {
			log.Println("❌ntrip caster client auth send data error:", err)
			_ = conn.Close()
			return nil
		}
		if !authTag {
			_ = conn.Close()
		} else {
			return NewNtripChannelBean(mount, conn, password)
		}
	}
	return nil
}
