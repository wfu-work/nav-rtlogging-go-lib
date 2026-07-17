package tcp

// Canonical initialism aliases. The original TcpClient and NewTcps names
// remain available for backward compatibility.
type (
	TCPClient        = TcpClient
	TCPServer        = Server
	DisconnectFunc   = DisConnectFunc
	OnDisconnectFunc = DisConnectFunc
	ErrorFunc        = NetErrorFunc
)

func NewTCPClient(host string, port int) *TCPClient {
	return NewTcpClient(host, port)
}

func NewTCPServer(port int) *TCPServer {
	return NewTcps(port)
}

func NewTCPServerOnAddress(host string, port int) *TCPServer {
	return NewTcpsOnAddress(host, port)
}

func (c *TcpClient) OnDisconnect(f OnDisconnectFunc) {
	c.DisConnect(f)
}

func (c *TcpClient) OnError(f ErrorFunc) {
	c.NetError(f)
}

func (s *Server) OnDisconnect(f OnDisconnectFunc) {
	s.DisConnect(f)
}

func (s *Server) OnError(f ErrorFunc) {
	s.NetError(f)
}
