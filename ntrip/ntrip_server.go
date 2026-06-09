package ntrip

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// NtripServer represents a Ntrip server.
type NtripServer struct {
	Host           string
	Port           int
	Mount          string
	Username       string
	Password       string
	Conn           net.Conn
	onConnect      OnConnectFunc
	onDisConnect   OnDisConnectFunc
	onDataCallback OnDataFunc
}

// NewNtripServer creates a new NtripClient.
func NewNtripServer(host string, port int, mount string, username string, password string) *NtripServer {
	return &NtripServer{
		Host:     host,
		Port:     port,
		Mount:    mount,
		Username: username,
		Password: password,
	}
}

// OnConnect 设置连接回调
func (s *NtripServer) OnConnect(f OnConnectFunc) {
	s.onConnect = f
}

// DisConnect 设置断开回调
func (s *NtripServer) DisConnect(f OnDisConnectFunc) {
	s.onDisConnect = f
}

// OnDataCallback 设置断开回调
func (s *NtripServer) OnDataCallback(f OnDataFunc) {
	s.onDataCallback = f
}

func (s *NtripServer) Start() error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", s.Host, s.Port), 5*time.Second)
	if err != nil {
		fmt.Println("❌ntrip server start error: ", err, "exit!")
		return errors.New("ntrip server start error")
	}
	enableTCPKeepAlive(conn)

	authMsg := createNtripServerAuthMsg(s.Mount, s.Password)
	_, err = conn.Write([]byte(authMsg))
	if err != nil {
		fmt.Println("❌ntrip server write error: ", err, "exit!")
		_ = conn.Close()
		return errors.New("ntrip server write error")
	}
	fmt.Println("✅ntrip server send auth msg for mount: ", s.Mount)
	go s.handleConn(conn)
	return nil
}

func (s *NtripServer) handleConn(conn net.Conn) {
	defer func() {
		if s.Conn == conn {
			s.Conn = nil
		}
		_ = conn.Close()
	}()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("❌read ntrip server error: ", err)
			if err == io.EOF {
				s.Conn = nil
				fmt.Println("❌ntrip server close! ")
				if s.onDisConnect != nil {
					s.onDisConnect(conn.LocalAddr().String(), s.Mount)
				}
			}
			break
		}
		data := buf[:n]
		fmt.Println("==> ntrip server receive msg: ", string(data))
		if strings.Contains(string(data), "401 Unauthorized") {
			fmt.Println("❌ntrip server auth failed! ", s.Mount, s.Username, maskSecret(s.Password))
			if s.onDisConnect != nil {
				s.onDisConnect(conn.LocalAddr().String(), s.Mount)
			}
			return
		}
		if strings.Contains(string(data), "ICY 200 OK") {
			fmt.Println("✅ntrip server auth ok! ", s.Mount, s.Username, maskSecret(s.Password))
			s.Conn = conn
			if s.onConnect != nil {
				s.onConnect(conn.LocalAddr().String(), s.Mount, conn)
			}
		}
	}
}

// createNtripServerAuthMsg creates a Ntrip authentication message.
func createNtripServerAuthMsg(mountPoint, password string) string {
	head := fmt.Sprintf("SOURCE %s /%s\r\nSource-Agent: NTRIP NtripServerCMD/1.0\r\n",
		password, mountPoint)
	head += "\r\n"
	return head
}
