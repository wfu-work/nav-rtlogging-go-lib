package ntrip

import (
	"context"
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
	"unicode"
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

// DefaultNtripCasterBindAddress is used when NtripCasterConfig.BindAddress is empty.
const DefaultNtripCasterBindAddress = "127.0.0.1"

// NtripCasterConfig configures both sides of the embedded caster.
// Externally reachable bind addresses require explicit authentication callbacks.
// Zero total connection limits use the default of 1024 connections per side;
// zero per-IP limits disable per-IP limiting.
type NtripCasterConfig struct {
	// BindAddress controls the local address for both listeners. An empty value
	// uses DefaultNtripCasterBindAddress.
	BindAddress               string
	SourcePort                int
	ClientPort                int
	SourceAuth                OnAuthFunc
	ClientAuth                OnAuthFunc
	SourceTLSConfig           *tls.Config
	ClientTLSConfig           *tls.Config
	MaxSourceConnections      int
	MaxClientConnections      int
	MaxSourceConnectionsPerIP int
	MaxClientConnectionsPerIP int
	RequireActiveSource       bool
	RequireSourcetableAuth    bool
}

// InitNtripCaster starts a loopback-only caster with development credentials.
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
	return InitNtripCasterWithConfig(NtripCasterConfig{
		SourcePort: ntripCasterServerPort,
		ClientPort: ntripCasterClientPort,
	})
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
	config = config.withDefaults()
	if config.MaxSourceConnections < 0 || config.MaxClientConnections < 0 ||
		config.MaxSourceConnectionsPerIP < 0 || config.MaxClientConnectionsPerIP < 0 {
		return nil, nil, errors.New("ntrip caster connection limits cannot be negative")
	}
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

	server := NewNtripCasterServerOnAddress(config.BindAddress, config.SourcePort)
	if err := server.SetTLSConfig(config.SourceTLSConfig); err != nil {
		return nil, nil, err
	}
	if config.MaxSourceConnections > 0 {
		if err := server.SetMaxConnections(config.MaxSourceConnections); err != nil {
			return nil, nil, err
		}
	}
	if config.MaxSourceConnectionsPerIP > 0 {
		if err := server.SetMaxConnectionsPerIP(config.MaxSourceConnectionsPerIP); err != nil {
			return nil, nil, err
		}
	}
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
	if err := client.SetTLSConfig(config.ClientTLSConfig); err != nil {
		return nil, nil, err
	}
	if config.MaxClientConnections > 0 {
		if err := client.SetMaxConnections(config.MaxClientConnections); err != nil {
			return nil, nil, err
		}
	}
	if config.MaxClientConnectionsPerIP > 0 {
		if err := client.SetMaxConnectionsPerIP(config.MaxClientConnectionsPerIP); err != nil {
			return nil, nil, err
		}
	}
	client.SetRequireActiveSource(config.RequireActiveSource)
	client.SetRequireSourcetableAuth(config.RequireSourcetableAuth)
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

	casterInitMu.Lock()
	defer casterInitMu.Unlock()
	stopNtripCaster()

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

func (config NtripCasterConfig) withDefaults() NtripCasterConfig {
	config.BindAddress = strings.TrimSpace(config.BindAddress)
	if config.BindAddress == "" {
		config.BindAddress = DefaultNtripCasterBindAddress
	}
	return config
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

func dialNtrip(ctx context.Context, host string, port int, timeout time.Duration, tlsConfig *tls.Config) (net.Conn, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	address := ntripAddress(host, port)
	dialer := &net.Dialer{Timeout: timeout}
	if tlsConfig == nil {
		return dialer.DialContext(ctx, "tcp", address)
	}
	config := tlsConfig.Clone()
	if config.ServerName == "" {
		config.ServerName = host
	}
	tlsDialer := &tls.Dialer{NetDialer: dialer, Config: config}
	return tlsDialer.DialContext(ctx, "tcp", address)
}

func ntripAddress(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func validateMount(mount string) error {
	if strings.TrimSpace(mount) == "" {
		return errors.New("ntrip mount point is empty")
	}
	if strings.ContainsRune(mount, ';') || strings.IndexFunc(mount, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return fmt.Errorf("ntrip mount point contains an invalid character: %q", mount)
	}
	return nil
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		if ip := tcpAddr.IP.String(); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err == nil && host != "" {
		return strings.Trim(host, "[]")
	}
	if value := strings.TrimSpace(addr.String()); value != "" {
		return value
	}
	return "unknown"
}

func writeNtripRejection(conn net.Conn, response string) {
	if conn == nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(defaultNtripWriteTimeout))
	_ = WriteData(conn, []byte(response))
}

func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if err := validateGGAInput(latitude, longitude, altitude); err != nil {
		return "", err
	}
	return generateGGA(latitude, longitude, altitude), nil
}

func validateGGAInput(latitude, longitude, altitude float64) error {
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -90 || latitude > 90 {
		return fmt.Errorf("latitude must be a finite value in [-90, 90], got %v", latitude)
	}
	if math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return fmt.Errorf("longitude must be a finite value in [-180, 180], got %v", longitude)
	}
	if math.IsNaN(altitude) || math.IsInf(altitude, 0) {
		return fmt.Errorf("altitude must be finite, got %v", altitude)
	}
	return nil
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
