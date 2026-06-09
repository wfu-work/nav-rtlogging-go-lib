package ntrip

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type OnConnectFunc func(key string, mount string, conn net.Conn)
type OnDisConnectFunc func(key string, mount string)
type OnDataFunc func(key string, mount string, data []byte, extra string)
type OnNetErrorFunc func(err error)
type OnSizeFunc func(key string, mount string, conn net.Conn, size int)
type OnAuthFunc func(mount string, username string, password string) bool
type OnSpeedFunc func(mount string, speed int64)

var (
	ntripCasterServer *NtripCasterServer
	ntripCasterClient *NtripCasterClient
	mountBytes        = make(map[string]int64)
	mutex             sync.Mutex
)

func InitNtripCaster(ntripCasterServerPort int, ntripCasterClientPort int) (*NtripCasterServer, *NtripCasterClient) {
	StopNtripCaster()

	ntripCasterServer = NewNtripCasterServer(ntripCasterServerPort)
	ntripCasterServer.OnConnect(func(key string, mount string, conn net.Conn) {
		log.Println("✅ntrip caster server online: ", key, mount)
	})
	ntripCasterServer.DisConnect(func(key string, mount string) {
		deleteMountBytes(mount)
		log.Println("❌ntrip caster server offline: ", key, mount)
	})
	ntripCasterServer.OnSize(func(key string, mount string, conn net.Conn, size int) {
		//log.Println("✅ntrip caster server data size callback: ", key, mount, size)
		mutex.Lock()
		mountBytes[mount] += int64(size)
		mutex.Unlock()
	})
	ntripCasterServer.OnAuth(func(mount string, username string, password string) bool {
		log.Println("✅ntrip caster server auth data: ", mount, maskSecret(password))
		return mount == password
	})
	_ = ntripCasterServer.Start()

	ntripCasterClient = NewNtripCasterClient(ntripCasterClientPort)
	ntripCasterClient.OnConnect(func(key string, mount string, conn net.Conn) {
		log.Println("✅ntrip caster client online: ", key, mount)
	})
	ntripCasterClient.DisConnect(func(key string, mount string) {
		log.Println("❌ntrip caster client offline: ", key, mount)
	})
	ntripCasterClient.OnAuth(func(mount string, username string, password string) bool {
		log.Println("✅ntrip caster client auth data: ", mount, username, maskSecret(password))
		return mount == password && username == password
	})
	_ = ntripCasterClient.Start()
	//go doMountBytes(ntripCasterServer)
	ntripCasterServer.SetNtripCasterClient(ntripCasterClient)
	return ntripCasterServer, ntripCasterClient
}

func StopNtripCaster() {
	if ntripCasterServer != nil {
		_ = ntripCasterServer.Stop()
		ntripCasterServer = nil
	}
	if ntripCasterClient != nil {
		_ = ntripCasterClient.Stop()
		ntripCasterClient = nil
	}
}

func GetNtripCasterServer() *NtripCasterServer {
	return ntripCasterServer
}

func GetNtripCasterClient() *NtripCasterClient {
	return ntripCasterClient
}

func doMountBytes(ntripCasterServer *NtripCasterServer) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		for range ticker.C {
			mutex.Lock()
			for mount, size := range mountBytes {
				if size == 0 {
					delete(mountBytes, mount)
					continue
				}
				speedBps := size
				speedKbps := float64(size*8) / 1024
				mountBytes[mount] = 0
				fmt.Printf("📶挂载点速率 %s: %d B/s (%.2f kbps)\n", mount, speedBps, speedKbps)
				if ntripCasterServer.onSpeed != nil {
					ntripCasterServer.onSpeed(mount, speedBps)
				}
			}
			mutex.Unlock()
		}
	}
}

func deleteMountBytes(mount string) {
	mutex.Lock()
	delete(mountBytes, mount)
	mutex.Unlock()
}

func enableTCPKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(defaultNtripKeepAlivePeriod)
}

func setReadDeadline(conn net.Conn, timeout time.Duration) {
	if conn == nil {
		return
	}
	if timeout <= 0 {
		_ = conn.SetReadDeadline(time.Time{})
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
}

// NowNtripDate 获取ntrip时间
func NowNtripDate() string {
	now := time.Now()
	formattedDate := now.Format("2006/01/02 15:04:05")
	return formattedDate
}

func WriteData(conn net.Conn, bytes []byte) error {
	if conn == nil {
		fmt.Printf("❌ntrip caster 发送数据异常: %s\n", "ntrip caster not connected")
		return errors.New("ntrip caster not connected")
	}
	_, err := conn.Write(bytes)
	if err != nil {
		fmt.Printf("❌ntrip caster 发送数据异常: %v\n", err)
		return fmt.Errorf("❌ntrip caster send data conn write error: %v", err)
	}
	return nil
}

// DecimalToNMEACoords 经纬度格式转换（十进制度 -> 度分制）
func DecimalToNMEACoords(decimal float64, isLatitude bool) (string, string) {
	dir := ""
	degrees := int(decimal)
	minutes := (decimal - float64(degrees)) * 60.0
	if isLatitude {
		dir = "N"
		if decimal < 0 {
			dir = "S"
			degrees = -degrees
		}
		return fmt.Sprintf("%02d%06.3f", degrees, minutes), dir
	} else {
		dir = "E"
		if decimal < 0 {
			dir = "W"
			degrees = -degrees
		}
		return fmt.Sprintf("%03d%06.3f", degrees, minutes), dir
	}
}

// GenerateGGA GGA语句生成器
func GenerateGGA(latitude, longitude float64, altitude float64) string {
	now := time.Now().UTC()
	timeStr := fmt.Sprintf("%02d%02d%02d.00", now.Hour(), now.Minute(), now.Second())
	latStr, latDir := DecimalToNMEACoords(latitude, true)
	lonStr, lonDir := DecimalToNMEACoords(longitude, false)
	// 默认参数：定位质量 1，12颗卫星，水平精度1.0，高度单位M
	gga := fmt.Sprintf("GPGGA,%s,%s,%s,%s,%s,1,12,1.0,%.1f,M,0.0,M,,", timeStr, latStr, latDir, lonStr, lonDir, altitude)
	checksum := byte(0)
	for i := 0; i < len(gga); i++ {
		checksum ^= gga[i]
	}
	return fmt.Sprintf("$%s*%02X\r\n", gga, checksum)
}
