package ntrip

import "net"

// Canonical initialism aliases. The original Ntrip-prefixed API remains
// available for backward compatibility.
type (
	NTRIPClient       = NtripClient
	NTRIPServer       = NtripServer
	NTRIPCasterServer = NtripCasterServer
	NTRIPCasterClient = NtripCasterClient
	NTRIPCasterConfig = NtripCasterConfig
	NTRIPChannelBean  = NtripChannelBean
	SafeNTRIPMap      = SafeNtripMap
	OnDisconnectFunc  = OnDisConnectFunc
	OnErrorFunc       = OnNetErrorFunc
)

func NewLocalNTRIPClient(mount string) *NTRIPClient {
	return NewLocalNtripClient(mount)
}

func NewNTRIPClient(host string, port int, mount, username, password string) *NTRIPClient {
	return NewNtripClient(host, port, mount, username, password)
}

func NewNTRIPClientExtra(host string, port int, mount, username, password, extra string) *NTRIPClient {
	return NewNtripClientExtra(host, port, mount, username, password, extra)
}

func NewNTRIPClientGGAExtra(host string, port int, mount, username, password string, latitude, longitude, altitude float64, extra string) *NTRIPClient {
	return NewNtripClientGgaExtra(host, port, mount, username, password, latitude, longitude, altitude, extra)
}

func NewNTRIPServer(host string, port int, mount, username, password string) *NTRIPServer {
	return NewNtripServer(host, port, mount, username, password)
}

func NewNTRIPCasterServer(port int) *NTRIPCasterServer {
	return NewNtripCasterServer(port)
}

func NewNTRIPCasterServerOnAddress(host string, port int) *NTRIPCasterServer {
	return NewNtripCasterServerOnAddress(host, port)
}

func NewNTRIPCasterClient(port int) *NTRIPCasterClient {
	return NewNtripCasterClient(port)
}

func NewNTRIPCasterClientOnAddress(host string, port int) *NTRIPCasterClient {
	return NewNtripCasterClientOnAddress(host, port)
}

func NewNTRIPChannelBean(mount string, conn net.Conn, extra string) *NTRIPChannelBean {
	return NewNtripChannelBean(mount, conn, extra)
}

func NewSafeNTRIPMap() *SafeNTRIPMap {
	return NewSafeNtripMap()
}

func InitNTRIPCaster(sourcePort, clientPort int) (*NTRIPCasterServer, *NTRIPCasterClient) {
	return InitNtripCaster(sourcePort, clientPort)
}

func InitNTRIPCasterWithError(sourcePort, clientPort int) (*NTRIPCasterServer, *NTRIPCasterClient, error) {
	return InitNtripCasterWithError(sourcePort, clientPort)
}

func InitNTRIPCasterWithAddress(address string, sourcePort, clientPort int) (*NTRIPCasterServer, *NTRIPCasterClient, error) {
	return InitNtripCasterWithAddress(address, sourcePort, clientPort)
}

func InitNTRIPCasterWithConfig(config NTRIPCasterConfig) (*NTRIPCasterServer, *NTRIPCasterClient, error) {
	return InitNtripCasterWithConfig(config)
}

func StopNTRIPCaster() {
	StopNtripCaster()
}

func GetNTRIPCasterServer() *NTRIPCasterServer {
	return GetNtripCasterServer()
}

func GetNTRIPCasterClient() *NTRIPCasterClient {
	return GetNtripCasterClient()
}

func (c *NtripClient) OnDisconnect(f OnDisconnectFunc) {
	c.DisConnect(f)
}

func (c *NtripClient) OnError(f OnErrorFunc) {
	c.OnNetErrorCallback(f)
}

func (c *NtripClient) OnData(f OnDataFunc) {
	c.OnDataCallback(f)
}

func (s *NtripServer) OnDisconnect(f OnDisconnectFunc) {
	s.DisConnect(f)
}

func (s *NtripServer) OnError(f OnErrorFunc) {
	s.OnNetErrorCallback(f)
}

func (s *NtripServer) OnData(f OnDataFunc) {
	s.OnDataCallback(f)
}

func (s *NtripCasterServer) OnDisconnect(f OnDisconnectFunc) {
	s.DisConnect(f)
}

func (s *NtripCasterServer) OnError(f OnErrorFunc) {
	s.NetError(f)
}

func (s *NtripCasterClient) OnDisconnect(f OnDisconnectFunc) {
	s.DisConnect(f)
}

func (s *NtripCasterClient) OnError(f OnErrorFunc) {
	s.NetError(f)
}
