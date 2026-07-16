package ntrip

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
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

// Logger is the minimal logging contract used by this package.
type Logger interface {
	Println(v ...any)
	Printf(format string, v ...any)
}

var (
	ntripCasterServer *NtripCasterServer
	ntripCasterClient *NtripCasterClient
	casterMu          sync.RWMutex
	casterInitMu      sync.Mutex
	loggerMu          sync.RWMutex
	packageLogger     Logger = log.Default()
)

// SetLogger replaces the package logger. Passing nil disables library logs.
func SetLogger(logger Logger) {
	loggerMu.Lock()
	packageLogger = logger
	loggerMu.Unlock()
}

func logPrintln(v ...any) {
	loggerMu.RLock()
	logger := packageLogger
	loggerMu.RUnlock()
	if logger != nil {
		logger.Println(v...)
	}
}

func logPrintf(format string, v ...any) {
	loggerMu.RLock()
	logger := packageLogger
	loggerMu.RUnlock()
	if logger != nil {
		logger.Printf(format, v...)
	}
}

// NtripCasterConfig configures both sides of the embedded caster.
// Externally reachable bind addresses require explicit authentication callbacks.
type NtripCasterConfig struct {
	BindAddress string
	SourcePort  int
	ClientPort  int
	SourceAuth  OnAuthFunc
	ClientAuth  OnAuthFunc
}

func InitNtripCaster(ntripCasterServerPort int, ntripCasterClientPort int) (*NtripCasterServer, *NtripCasterClient) {
	server, client, err := InitNtripCasterWithError(ntripCasterServerPort, ntripCasterClientPort)
	if err != nil {
		logPrintln("❌ntrip caster init error:", err)
	}
	return server, client
}

// InitNtripCasterWithError initializes both sides of the embedded caster and
// reports listener failures. If either listener fails, any partially started
// listener is stopped before the function returns.
func InitNtripCasterWithError(ntripCasterServerPort int, ntripCasterClientPort int) (*NtripCasterServer, *NtripCasterClient, error) {
	return InitNtripCasterWithAddress("127.0.0.1", ntripCasterServerPort, ntripCasterClientPort)
}

// InitNtripCasterWithAddress initializes both caster listeners on bindAddress.
// Only loopback addresses may use the development authentication defaults.
func InitNtripCasterWithAddress(bindAddress string, ntripCasterServerPort int, ntripCasterClientPort int) (*NtripCasterServer, *NtripCasterClient, error) {
	return InitNtripCasterWithConfig(NtripCasterConfig{
		BindAddress: bindAddress,
		SourcePort:  ntripCasterServerPort,
		ClientPort:  ntripCasterClientPort,
	})
}

// InitNtripCasterWithConfig initializes both caster listeners. Explicit source
// and client authentication callbacks are mandatory for non-loopback binds.
func InitNtripCasterWithConfig(config NtripCasterConfig) (*NtripCasterServer, *NtripCasterClient, error) {
	loopback := isLoopbackBindAddress(config.BindAddress)
	if !loopback && (config.SourceAuth == nil || config.ClientAuth == nil) {
		return nil, nil, errors.New("external ntrip caster listeners require explicit source and client authentication callbacks")
	}
	sourceAuth := config.SourceAuth
	if sourceAuth == nil {
		sourceAuth = func(mount, username, password string) bool { return mount == password }
	}
	clientAuth := config.ClientAuth
	if clientAuth == nil {
		clientAuth = func(mount, username, password string) bool {
			return mount == password && username == password
		}
	}

	casterInitMu.Lock()
	defer casterInitMu.Unlock()
	stopNtripCaster()

	server := NewNtripCasterServerOnAddress(config.BindAddress, config.SourcePort)
	server.OnConnect(func(key string, mount string, conn net.Conn) {
		logPrintln("✅ntrip caster server online: ", key, mount)
	})
	server.DisConnect(func(key string, mount string) {
		logPrintln("❌ntrip caster server offline: ", key, mount)
	})
	server.OnAuth(func(mount string, username string, password string) bool {
		logPrintln("✅ntrip caster server auth data: ", mount, maskSecret(password))
		return sourceAuth(mount, username, password)
	})

	client := NewNtripCasterClientOnAddress(config.BindAddress, config.ClientPort)
	client.OnConnect(func(key string, mount string, conn net.Conn) {
		logPrintln("✅ntrip caster client online: ", key, mount)
	})
	client.DisConnect(func(key string, mount string) {
		logPrintln("❌ntrip caster client offline: ", key, mount)
	})
	client.OnAuth(func(mount string, username string, password string) bool {
		logPrintln("✅ntrip caster client auth data: ", mount, username, maskSecret(password))
		return clientAuth(mount, username, password)
	})
	server.SetNtripCasterClient(client)

	if err := server.Start(); err != nil {
		return server, client, fmt.Errorf("start ntrip caster source listener: %w", err)
	}
	if err := client.Start(); err != nil {
		_ = server.Stop()
		return server, client, fmt.Errorf("start ntrip caster client listener: %w", err)
	}

	casterMu.Lock()
	ntripCasterServer = server
	ntripCasterClient = client
	casterMu.Unlock()
	return server, client, nil
}

func isLoopbackBindAddress(address string) bool {
	if strings.EqualFold(address, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(address, "[]"))
	return ip != nil && ip.IsLoopback()
}

func StopNtripCaster() {
	casterInitMu.Lock()
	defer casterInitMu.Unlock()
	stopNtripCaster()
}

func stopNtripCaster() {
	casterMu.Lock()
	server := ntripCasterServer
	client := ntripCasterClient
	ntripCasterServer = nil
	ntripCasterClient = nil
	casterMu.Unlock()

	if server != nil {
		_ = server.Stop()
	}
	if client != nil {
		_ = client.Stop()
	}
}

func GetNtripCasterServer() *NtripCasterServer {
	casterMu.RLock()
	defer casterMu.RUnlock()
	return ntripCasterServer
}

func GetNtripCasterClient() *NtripCasterClient {
	casterMu.RLock()
	defer casterMu.RUnlock()
	return ntripCasterClient
}

func enableTCPKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(defaultNtripKeepAlivePeriod)
}

func dialNtrip(host string, port int, timeout time.Duration, tlsConfig *tls.Config) (net.Conn, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	address := ntripAddress(host, port)
	dialer := &net.Dialer{Timeout: timeout}
	if tlsConfig == nil {
		return dialer.Dial("tcp", address)
	}
	config := tlsConfig.Clone()
	if config.ServerName == "" {
		config.ServerName = host
	}
	return tls.DialWithDialer(dialer, "tcp", address, config)
}

func ntripAddress(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func validateMount(mount string) error {
	if strings.TrimSpace(mount) == "" {
		return errors.New("ntrip mount point is empty")
	}
	if strings.ContainsAny(mount, "\r\n\t ") {
		return fmt.Errorf("ntrip mount point contains invalid whitespace: %q", mount)
	}
	return nil
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
	return time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
}

func WriteData(conn net.Conn, bytes []byte) error {
	if conn == nil {
		return errors.New("ntrip caster not connected")
	}
	_, err := writeAll(conn, bytes)
	if err != nil {
		return fmt.Errorf("ntrip connection write: %w", err)
	}
	return nil
}

func writeAll(conn net.Conn, data []byte) (int, error) {
	written := 0
	for written < len(data) {
		n, err := conn.Write(data[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// DecimalToNMEACoords 经纬度格式转换（十进制度 -> 度分制）
func DecimalToNMEACoords(decimal float64, isLatitude bool) (string, string) {
	absolute := math.Abs(decimal)
	degrees := int(absolute)
	minutes := (absolute - float64(degrees)) * 60.0
	if isLatitude {
		dir := "N"
		if decimal < 0 {
			dir = "S"
		}
		return fmt.Sprintf("%02d%06.3f", degrees, minutes), dir
	} else {
		dir := "E"
		if decimal < 0 {
			dir = "W"
		}
		return fmt.Sprintf("%03d%06.3f", degrees, minutes), dir
	}
}

// GenerateGGA GGA语句生成器
func GenerateGGA(latitude, longitude float64, altitude float64) string {
	return generateGGA(latitude, longitude, altitude)
}

// GenerateGGAChecked validates latitude, longitude, and altitude before
// generating a GGA sentence.
func GenerateGGAChecked(latitude, longitude float64, altitude float64) (string, error) {
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -90 || latitude > 90 {
		return "", fmt.Errorf("latitude must be a finite value in [-90, 90], got %v", latitude)
	}
	if math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return "", fmt.Errorf("longitude must be a finite value in [-180, 180], got %v", longitude)
	}
	if math.IsNaN(altitude) || math.IsInf(altitude, 0) {
		return "", fmt.Errorf("altitude must be finite, got %v", altitude)
	}
	return generateGGA(latitude, longitude, altitude), nil
}

func generateGGA(latitude, longitude float64, altitude float64) string {
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
