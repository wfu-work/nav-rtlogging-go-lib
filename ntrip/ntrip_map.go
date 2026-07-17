package ntrip

import (
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultNtripSendQueueSize     = 256
	defaultNtripReadBufferSize    = 4096
	defaultNtripAuthTimeout       = 10 * time.Second
	defaultNtripSourceIdleTimeout = 90 * time.Second
	defaultNtripClientIdleTimeout = 60 * time.Second
	defaultNtripWriteTimeout      = 1 * time.Second
	defaultNtripKeepAlivePeriod   = 2 * time.Minute
	defaultNtripMaxHeaderSize     = 8192
	defaultNtripMaxConnections    = 1024
)

type NtripChannelBean struct {
	mount     string
	conn      net.Conn
	extra     string
	send      chan []byte
	quit      chan struct{}
	bytesSent uint64
	packets   uint64
	dropped   uint64
	once      sync.Once
	closed    uint32
}

// NewNtripChannelBean 构造并启动写协程
func NewNtripChannelBean(mount string, conn net.Conn, extra string) *NtripChannelBean {
	bean := &NtripChannelBean{
		mount: mount,
		conn:  conn,
		extra: extra,
		send:  make(chan []byte, defaultNtripSendQueueSize),
		quit:  make(chan struct{}),
	}
	go bean.writer()
	return bean
}

func (c *NtripChannelBean) writer() {
	defer func() {
		logPrintf("🛑channel bean writer for %s closed\n", c.mount)
		c.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				logPrintf("❌channel bean send to %s is : %v\n", c.mount, false)
				return
			}
			if len(msg) == 0 {
				continue
			}
			written := 0
			attempt := 0
			for written < len(msg) {
				_ = c.conn.SetWriteDeadline(time.Now().Add(defaultNtripWriteTimeout))
				n, err := c.conn.Write(msg[written:])
				written += n
				if err == nil {
					if n == 0 {
						logPrintf("❌write failed to %s: %v\n", c.mount, io.ErrShortWrite)
						return
					}
					attempt = 0
					continue
				}
				if nerr, ok := err.(net.Error); ok && nerr.Timeout() && attempt < 3 {
					attempt++
					logPrintf("⚠️retry #%d write timeout to %s...\n", attempt, c.mount)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				logPrintf("❌write failed to %s: %v (fatal)\n", c.mount, err)
				return
			}
			_ = c.conn.SetWriteDeadline(time.Time{})
			atomic.AddUint64(&c.bytesSent, uint64(len(msg)))
			atomic.AddUint64(&c.packets, 1)
		case <-c.quit:
			logPrintf("🟡quit signal for %s\n", c.mount)
			return
		}
	}
}

// Send 发送数据
func (c *NtripChannelBean) Send(data []byte) {
	if atomic.LoadUint32(&c.closed) == 1 {
		return
	}
	data = cloneBytes(data)
	select {
	case c.send <- data:
		return
	default:
	}

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	select {
	case c.send <- data:
	case <-timer.C:
		atomic.AddUint64(&c.dropped, 1)
		logPrintf("🛑send timeout for %s, slow consumer detected\n", c.mount)
	}
}

// SendLoss 发送数据，队列满时丢弃旧数据，保留最新实时数据。
func (c *NtripChannelBean) SendLoss(data []byte) {
	c.sendLossOwned(cloneBytes(data))
}

// sendLossOwned queues immutable data without copying it. The caller must not
// mutate data after this call; it may be shared by multiple subscribers.
func (c *NtripChannelBean) sendLossOwned(data []byte) {
	if len(data) == 0 || atomic.LoadUint32(&c.closed) == 1 {
		return
	}
	select {
	case c.send <- data:
	default:
		select {
		case <-c.send:
			atomic.AddUint64(&c.dropped, 1)
		default:
		}
		select {
		case c.send <- data:
		default:
			atomic.AddUint64(&c.dropped, 1)
			logPrintf("🛑channel full for %s, drop latest packet\n", c.mount)
		}
	}
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	return append([]byte(nil), data...)
}

// Close 关闭连接
func (c *NtripChannelBean) Close() {
	c.once.Do(func() {
		atomic.StoreUint32(&c.closed, 1)
		if c.quit != nil {
			close(c.quit)
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
		logPrintf("🛑closed connection for mount %s\n", c.mount)
	})
}

// BytesSent returns the number of payload bytes written successfully.
func (c *NtripChannelBean) BytesSent() uint64 {
	return atomic.LoadUint64(&c.bytesSent)
}

// PacketsSent returns the number of payload packets written successfully.
func (c *NtripChannelBean) PacketsSent() uint64 {
	return atomic.LoadUint64(&c.packets)
}

// PacketsDropped returns the number of packets discarded due to backpressure.
func (c *NtripChannelBean) PacketsDropped() uint64 {
	return atomic.LoadUint64(&c.dropped)
}

type SafeNtripMap struct {
	mu      sync.RWMutex
	data    map[string]*NtripChannelBean
	byMount map[string]map[string]*NtripChannelBean
}

func NewSafeNtripMap() *SafeNtripMap {
	return &SafeNtripMap{
		data:    make(map[string]*NtripChannelBean),
		byMount: make(map[string]map[string]*NtripChannelBean),
	}
}

func (s *SafeNtripMap) Get(key string) *NtripChannelBean {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if ok {
		return val
	}
	return nil
}

func (s *SafeNtripMap) Set(key string, val *NtripChannelBean) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()
	if old := s.data[key]; old != nil {
		s.deleteMountIndexLocked(key, old.mount)
	}
	if val == nil {
		delete(s.data, key)
		return
	}
	s.data[key] = val
	if s.byMount[val.mount] == nil {
		s.byMount[val.mount] = make(map[string]*NtripChannelBean)
	}
	s.byMount[val.mount][key] = val
}

// SetIfMountAbsent stores val only when no other entry owns the same mount.
// It atomically enforces the single-source-per-mount invariant.
func (s *SafeNtripMap) SetIfMountAbsent(key string, val *NtripChannelBean) bool {
	if val == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()
	if existing := s.byMount[val.mount]; len(existing) > 0 {
		return false
	}
	if old := s.data[key]; old != nil {
		s.deleteMountIndexLocked(key, old.mount)
	}
	s.data[key] = val
	s.byMount[val.mount] = map[string]*NtripChannelBean{key: val}
	return true
}

func (s *SafeNtripMap) ensureInitializedLocked() {
	if s.data == nil {
		s.data = make(map[string]*NtripChannelBean)
	}
	if s.byMount == nil {
		s.byMount = make(map[string]map[string]*NtripChannelBean)
	}
}

func (s *SafeNtripMap) Delete(key string) *NtripChannelBean {
	s.mu.Lock()
	defer s.mu.Unlock()
	val := s.data[key]
	delete(s.data, key)
	if val != nil {
		s.deleteMountIndexLocked(key, val.mount)
	}
	return val
}

func (s *SafeNtripMap) deleteMountIndexLocked(key string, mount string) {
	mountMap := s.byMount[mount]
	if mountMap == nil {
		return
	}
	delete(mountMap, key)
	if len(mountMap) == 0 {
		delete(s.byMount, mount)
	}
}

func (s *SafeNtripMap) GetByMount(mount string) []*NtripChannelBean {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*NtripChannelBean, 0, len(s.byMount[mount]))
	for _, bean := range s.byMount[mount] {
		result = append(result, bean)
	}
	return result
}

// HasMount reports whether at least one connection currently owns mount.
func (s *SafeNtripMap) HasMount(mount string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byMount[mount]) > 0
}

func (s *SafeNtripMap) ForEachByMount(mount string, f func(*NtripChannelBean)) {
	s.mu.RLock()
	beans := make([]*NtripChannelBean, 0, len(s.byMount[mount]))
	for _, bean := range s.byMount[mount] {
		beans = append(beans, bean)
	}
	s.mu.RUnlock()
	for _, bean := range beans {
		f(bean)
	}
}

// BroadcastLossByMount sends one shared immutable payload to all subscribers
// on mount. It avoids one payload allocation and one slice allocation per
// subscriber while preserving the real-time drop-oldest policy.
func (s *SafeNtripMap) BroadcastLossByMount(mount string, data []byte) int {
	if len(data) == 0 {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, bean := range s.byMount[mount] {
		if bean == nil || bean.conn == nil {
			continue
		}
		bean.sendLossOwned(data)
		count++
	}
	return count
}

func (s *SafeNtripMap) GetMountList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.byMount))
	for mount := range s.byMount {
		result = append(result, mount)
	}
	sort.Strings(result)
	return result
}

func (s *SafeNtripMap) QueryMountList(mount string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.byMount))
	for candidate := range s.byMount {
		if strings.Contains(candidate, mount) {
			result = append(result, candidate)
		}
	}
	sort.Strings(result)
	return result
}

func (s *SafeNtripMap) CloseAll() {
	s.mu.Lock()
	beans := make([]*NtripChannelBean, 0, len(s.data))
	for key, bean := range s.data {
		if bean != nil {
			beans = append(beans, bean)
		}
		delete(s.data, key)
	}
	for mount := range s.byMount {
		delete(s.byMount, mount)
	}
	s.mu.Unlock()

	for _, bean := range beans {
		bean.Close()
	}
}
