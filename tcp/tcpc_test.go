package tcp

import (
	"net"
	"testing"
)

func TestTcpClientStopClosesExistingConn(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	client := &TcpClient{conn: clientConn}

	if err := client.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if client.conn != nil {
		t.Fatal("expected client conn to be nil after Stop")
	}

	if _, err := clientConn.Write([]byte("x")); err == nil {
		t.Fatal("expected write to fail after Stop")
	}
}

func TestTcpServerStopClosesListener(t *testing.T) {
	server := NewTcps(0)
	if err := server.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if server.ln != nil {
		t.Fatal("expected listener to be nil after Stop")
	}
}
