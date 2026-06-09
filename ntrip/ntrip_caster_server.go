package ntrip

import (
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

// NtripCasterServer 封装结构
type NtripCasterServer struct {
	addr              string
	ln                net.Listener
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
	stopOnce          sync.Once
	mu                sync.Mutex
}

// NewNtripCasterServer 创建新 Server 实例
func NewNtripCasterServer(port int) *NtripCasterServer {
	return &NtripCasterServer{
		addr:     fmt.Sprintf(":%d", port),
		ntripMap: NewSafeNtripMap(),
		done:     make(chan struct{}),
	}
}

// OnConnect 设置连接回调
func (s *NtripCasterServer) OnConnect(f OnConnectFunc) {
	s.onConnect = f
}

// DisConnect 设置断开回调
func (s *NtripCasterServer) DisConnect(f OnDisConnectFunc) {
	s.disConnect = f
}

// OnData 设置数据回调
func (s *NtripCasterServer) OnData(f OnDataFunc) {
	s.onData = f
}

// OnSize 设置数据大小回调
func (s *NtripCasterServer) OnSize(f OnSizeFunc) {
	s.onSize = f
}

// OnAuth 设置认证回调
func (s *NtripCasterServer) OnAuth(f OnAuthFunc) {
	s.onAuth = f
}

// OnSpeed 设置速率回调
func (s *NtripCasterServer) OnSpeed(f OnSpeedFunc) {
	s.onSpeed = f
}

// NetError 设置网络错误回调
func (s *NtripCasterServer) NetError(f OnNetErrorFunc) {
	s.onNetError = f
}

// SetNtripCasterClient 关联caster
func (s *NtripCasterServer) SetNtripCasterClient(f *NtripCasterClient) {
	s.ntripCasterClient = f
}

func (s *NtripCasterServer) GetNtripMap() *SafeNtripMap {
	return s.ntripMap
}

// Start 启动服务
func (s *NtripCasterServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Println("❌ntrip caster server net error:", err)
		if s.onNetError != nil {
			s.onNetError(err)
		}
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	log.Println("✅ntrip caster server listening on", s.addr)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
				}
				log.Println("❌ntrip caster server accept error:", err)
				continue
			}
			go s.handleConn(conn)
		}
	}()
	return nil
}

func (s *NtripCasterServer) Stop() error {
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
func (s *NtripCasterServer) handleConn(conn net.Conn) {
	enableTCPKeepAlive(conn)
	key := conn.RemoteAddr().String()
	defer func() {
		ntripChannelVo := s.ntripMap.Delete(key)
		if ntripChannelVo != nil {
			deleteMountBytes(ntripChannelVo.mount)
			if s.disConnect != nil {
				s.disConnect(key, ntripChannelVo.mount)
			}
			log.Printf("❌ntrip caster server disconnect from: %s - %s", conn.RemoteAddr(), ntripChannelVo.mount)
		}
		_ = conn.Close()
	}()

	authenticated := false
	buf := make([]byte, defaultNtripReadBufferSize)
	for {
		if authenticated {
			setReadDeadline(conn, defaultNtripSourceIdleTimeout)
		} else {
			setReadDeadline(conn, defaultNtripAuthTimeout)
		}
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("❌ntrip caster server read error: ", err)
			break
		}
		bytes := buf[:n]
		ntripChannelBean := s.ntripMap.Get(key)
		if ntripChannelBean == nil {
			ntripChannelBean = s.auth(conn, bytes)
			if ntripChannelBean != nil {
				authenticated = true
				s.ntripMap.Set(key, ntripChannelBean)
				if s.onConnect != nil {
					s.onConnect(key, ntripChannelBean.mount, conn)
				}
			}
		} else {
			if s.ntripCasterClient != nil {
				s.ntripCasterClient.ntripMap.ForEachByMount(ntripChannelBean.mount, func(bean *NtripChannelBean) {
					if bean != nil && bean.conn != nil {
						bean.SendLoss(bytes)
					}
				})
			}
			if s.onData != nil {
				s.onData(key, ntripChannelBean.mount, bytes, ntripChannelBean.extra)
			}
		}
		if ntripChannelBean != nil && s.onSize != nil && len(bytes) > 0 {
			s.onSize(key, ntripChannelBean.mount, conn, len(bytes))
		}
	}
}

func (s *NtripCasterServer) auth(conn net.Conn, bytes []byte) *NtripChannelBean {
	dataStr := string(bytes)
	if dataStr != "" && (strings.HasPrefix(dataStr, "SOURCE") || strings.HasPrefix(dataStr, "POST")) {
		log.Println("✅ntrip caster server auth request received from: ", conn.RemoteAddr().String())
		var authTag = false
		var mount = ""
		var password = ""
		if strings.HasPrefix(dataStr, "SOURCE") && !strings.Contains(dataStr, "Source-Agent: NTRIP NtripLinux\r\nSTR:") {
			mount, password, authTag = s.authNtripServer1(dataStr)
		} else if strings.HasPrefix(dataStr, "SOURCE") && strings.Contains(dataStr, "Source-Agent: NTRIP NtripLinux\r\nSTR:") {
			mount, password, authTag = s.authNtripServer2(dataStr)
		} else {
			mount, password, authTag = s.authNtripServer3(dataStr)
		}
		var result string
		if authTag {
			result = "ICY 200 OK\r\nServer: Trimble-iGate/1.0\r\nDate:" + NowNtripDate() + "\r\n\r\n"
		} else {
			result = "HTTP/1.0 401 Unauthorized\r\n"
		}
		log.Println("✅ntrip caster server auth result: ", mount, maskSecret(password), result)
		err := WriteData(conn, []byte(result))
		if err != nil {
			fmt.Println("❌ntrip caster server auth send data error:", err)
			_ = conn.Close()
			return nil
		}
		if !authTag {
			_ = conn.Close()
		} else {
			return &NtripChannelBean{
				mount: strings.ReplaceAll(mount, "/", ""),
				conn:  conn,
			}
		}
	}
	return nil
}

func (s *NtripCasterServer) authNtripServer1(dataStr string) (string, string, bool) {
	var authTag = false
	splits := strings.Split(dataStr, "\r\n")
	if len(splits) >= 3 {
		authStr := splits[0]
		authSplits := strings.Fields(authStr)
		if len(authSplits) < 3 {
			return "", "", false
		}
		mountPoint := strings.ReplaceAll(authSplits[2], "/", "")
		password := authSplits[1]
		if mountPoint == password {
			authTag = true
		}
		if mountPoint != password {
			if s.onAuth != nil {
				authTag = s.onAuth(mountPoint, "", password)
			}
		}
		return mountPoint, password, authTag
	}
	return "", "", false
}

func (s *NtripCasterServer) authNtripServer2(dataStr string) (string, string, bool) {
	var authTag = false
	splits := strings.Split(dataStr, "\r\n")
	if len(splits) >= 3 {
		authStr := splits[0]
		authSplits := strings.Fields(authStr)
		if len(authSplits) < 3 {
			return "", "", false
		}
		mountPoint := strings.ReplaceAll(authSplits[2], "/", "")
		password := authSplits[1]
		if mountPoint == password {
			authTag = true
		}
		if mountPoint != password {
			if s.onAuth != nil {
				authTag = s.onAuth(mountPoint, "", password)
			}
		}
		return mountPoint, password, authTag
	}
	return "", "", false
}

func (s *NtripCasterServer) authNtripServer3(dataStr string) (string, string, bool) {
	var authTag = false
	var mount string
	var password string
	splits := strings.Split(dataStr, "\r\n")
	if len(splits) > 0 {
		parts := strings.Fields(splits[0])
		if len(parts) > 1 {
			mount = strings.ReplaceAll(parts[1], "/", "")
		}
	}
	var authorization = ""
	if len(splits) > 3 && strings.HasPrefix(splits[3], "Authorization:") {
		parts := strings.Fields(splits[3])
		if len(parts) > 2 {
			authorization = parts[2]
		}
	} else if len(splits) > 4 && strings.HasPrefix(splits[4], "Authorization:") {
		parts := strings.Fields(splits[4])
		if len(parts) > 2 {
			authorization = parts[2]
		}
	}
	if authorization == "" {
		return mount, "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(authorization)
	if err != nil {
		log.Println("❌ntrip caster server auth error:", err)
		return "", "", false
	}
	auth := string(decoded)
	parts := strings.SplitN(auth, ":", 2)
	if len(parts) == 2 {
		password = parts[1]
	}
	if mount == password {
		authTag = true
	} else {
		if s.onAuth != nil {
			authTag = s.onAuth(mount, "", password)
		}
	}
	return mount, password, authTag
}
