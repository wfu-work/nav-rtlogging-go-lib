//go:build integration

package ntrip

import (
	"fmt"
	"net"
	"testing"

	"github.com/wfu-work/nav-rtlogging-go-lib/tcp"
)

func TestNtripClient(t *testing.T) {
	ntripClient := NewNtripClientGgaExtra("203.107.45.154", 8003, "AUTO", "qxykhy005741", "7147d20", 32.05330802, 119.61051377, 1929.9857, "")
	ntripClient.OnConnect(func(key string, mountPoint string, conn net.Conn) {
		fmt.Println("✅ntrip client online: ", key, mountPoint)
	})
	ntripClient.DisConnect(func(key string, mountPoint string) {
		fmt.Println("❌ntrip client offline: ", key, mountPoint)
		ntripClient.Retry()
	})
	ntripClient.OnNetErrorCallback(func(err error) {
		fmt.Println("❌ntrip client net error: ", err)
		ntripClient.Retry()
	})
	ntripClient.OnDataCallback(func(key string, mountPoint string, data []byte, extra string) {
		fmt.Println("==> ntrip client get data: ", key, mountPoint, string(data))
	})
	_ = ntripClient.Start()

	select {}
}

func TestNtripCasterServer(t *testing.T) {
	InitNtripCaster(9090, 9095)
	select {}
}

func TestTcpc(t *testing.T) {
	tcpc := tcp.NewTcpClient("203.107.45.154", 2101)
	tcpc.OnConnect(func(conn net.Conn) {
		fmt.Println("✅tcp client online: ", conn.RemoteAddr().String())
	})
	tcpc.DisConnect(func(conn net.Conn) {
		fmt.Println("❌tcp client offline: ", conn.RemoteAddr().String())
	})
	tcpc.NetError(func(err error) {
		fmt.Println("❌tcp client net error: ", err)
	})
	tcpc.OnData(func(conn net.Conn, data []byte) {
		fmt.Println("==> tcp client get data: ", conn.RemoteAddr().String(), string(data))
	})
	_ = tcpc.Start()
	select {}
}
