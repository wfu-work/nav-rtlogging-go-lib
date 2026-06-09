package ntrip

import (
	"log"
	"net"
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
)

type NtripChannelBean struct {
	mount     string
	conn      net.Conn
	extra     string
	send      chan []byte
	quit      chan struct{}
	bytesSent uint64
	packets   uint64
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
		log.Printf("🛑channel bean writer for %s closed\n", c.mount)
		c.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				log.Printf("❌channel bean send to %s is : %v\n", c.mount, false)
				return
			}
			if len(msg) == 0 {
				continue
			}
			wrote := false
			for attempt := 1; attempt <= 3; attempt++ {
				_ = c.conn.SetWriteDeadline(time.Now().Add(defaultNtripWriteTimeout))
				_, err := c.conn.Write(msg)
				if err == nil {
					wrote = true
					break
				}
				if nerr, ok := err.(net.Error); ok && (nerr.Timeout() || nerr.Temporary()) {
					log.Printf("⚠️retry #%d write timeout to %s...\n", attempt, c.mount)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				log.Printf("❌write failed to %s: %v (fatal)\n", c.mount, err)
				return
			}
			if !wrote {
				log.Printf("❌write timeout to %s after retries\n", c.mount)
				return
			}
			atomic.AddUint64(&c.bytesSent, uint64(len(msg)))
			atomic.AddUint64(&c.packets, 1)
		case <-c.quit:
			log.Printf("🟡quit signal for %s\n", c.mount)
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
		log.Printf("🛑send timeout for %s, slow consumer detected\n", c.mount)
	}
}

// SendLoss 发送数据，队列满时丢弃旧数据，保留最新实时数据。
func (c *NtripChannelBean) SendLoss(data []byte) {
	if atomic.LoadUint32(&c.closed) == 1 {
		return
	}
	data = cloneBytes(data)
	select {
	case c.send <- data:
	default:
		select {
		case <-c.send:
		default:
		}
		select {
		case c.send <- data:
		default:
			log.Printf("🛑channel full for %s, drop latest packet\n", c.mount)
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
		log.Printf("🛑closed connection for mount %s\n", c.mount)
	})
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
	if old := s.data[key]; old != nil {
		s.deleteMountIndexLocked(key, old.mount)
	}
	s.data[key] = val
	if val != nil {
		if s.byMount[val.mount] == nil {
			s.byMount[val.mount] = make(map[string]*NtripChannelBean)
		}
		s.byMount[val.mount][key] = val
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
	var result []*NtripChannelBean
	for _, bean := range s.byMount[mount] {
		result = append(result, bean)
	}
	return result
}

func (s *SafeNtripMap) ForEachByMount(mount string, f func(*NtripChannelBean)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, bean := range s.byMount[mount] {
		f(bean)
	}
}

func (s *SafeNtripMap) GetMountList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []string
	for _, bean := range s.data {
		result = append(result, bean.mount)
	}
	return result
}

func (s *SafeNtripMap) QueryMountList(mount string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []string
	for _, bean := range s.data {
		if strings.Contains(bean.mount, mount) {
			result = append(result, bean.mount)
		}
	}
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
		deleteMountBytes(bean.mount)
	}
}
