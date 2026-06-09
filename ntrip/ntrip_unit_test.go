package ntrip

import (
	"net"
	"testing"
	"time"
)

func TestAuthNtripServer2InvalidRequestDoesNotPanic(t *testing.T) {
	server := NewNtripCasterServer(0)

	mount, password, ok := server.authNtripServer2("SOURCE\r\nSource-Agent: NTRIP NtripLinux\r\nSTR:\r\n")

	if ok {
		t.Fatal("expected invalid request to fail auth")
	}
	if mount != "" || password != "" {
		t.Fatalf("expected empty mount and password, got %q and %q", mount, password)
	}
}

func TestAuthNtripServer2ValidRequest(t *testing.T) {
	server := NewNtripCasterServer(0)

	mount, password, ok := server.authNtripServer2("SOURCE MOUNT1 /MOUNT1\r\nSource-Agent: NTRIP NtripLinux\r\nSTR:\r\n")

	if !ok {
		t.Fatal("expected matching mount and password to pass auth")
	}
	if mount != "MOUNT1" || password != "MOUNT1" {
		t.Fatalf("unexpected auth values: mount=%q password=%q", mount, password)
	}
}

func TestSafeNtripMapQueryMountListUsesSubstring(t *testing.T) {
	m := NewSafeNtripMap()
	m.Set("one", &NtripChannelBean{mount: "BASE_ABC_001"})
	m.Set("two", &NtripChannelBean{mount: "BASE_XYZ_001"})

	got := m.QueryMountList("ABC")

	if len(got) != 1 || got[0] != "BASE_ABC_001" {
		t.Fatalf("expected only BASE_ABC_001, got %#v", got)
	}
}

func TestNtripClientStopWithoutConnectionDoesNotPanic(t *testing.T) {
	client := NewNtripClient("127.0.0.1", 2101, "MOUNT", "user", "password")

	client.Stop()
	client.Stop()
}

func TestNtripClientRetryStopsOnStop(t *testing.T) {
	client := NewNtripClient("127.0.0.1", 1, "MOUNT", "user", "password")

	client.Retry()
	if !client.retrying.Load() {
		t.Fatal("expected retry loop to start")
	}

	client.Stop()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !client.retrying.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected retry loop to stop after Stop")
}

func TestNtripClientDeliversPayloadAfterAuthHeader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	client := NewNtripClient("127.0.0.1", 2101, "MOUNT", "user", "password")
	defer client.Stop()

	dataCh := make(chan []byte, 1)
	client.OnDataCallback(func(key string, mount string, data []byte, extra string) {
		dataCh <- data
	})

	go client.handleConn(clientConn)

	if _, err := serverConn.Write([]byte("ICY 200 OK\r\nServer: test\r\n\r\nrtcm")); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	select {
	case got := <-dataCh:
		if string(got) != "rtcm" {
			t.Fatalf("expected rtcm payload, got %q", string(got))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for payload")
	}
}

func TestNtripClientSupportsSingleLineIcyResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	client := NewNtripClient("127.0.0.1", 2101, "MOUNT", "user", "password")
	defer client.Stop()

	dataCh := make(chan []byte, 1)
	client.OnDataCallback(func(key string, mount string, data []byte, extra string) {
		dataCh <- data
	})

	go client.handleConn(clientConn)

	payload := []byte{0xd3, 0x00, 0x01}
	if _, err := serverConn.Write(append([]byte("ICY 200 OK\r\n"), payload...)); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	select {
	case got := <-dataCh:
		if string(got) != string(payload) {
			t.Fatalf("expected payload %v, got %v", payload, got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for payload")
	}
}

func TestNtripCasterServerStopClosesListener(t *testing.T) {
	server := NewNtripCasterServer(0)
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

func TestNtripCasterClientStopClosesListener(t *testing.T) {
	client := NewNtripCasterClient(0)
	if err := client.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if err := client.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if client.ln != nil {
		t.Fatal("expected listener to be nil after Stop")
	}
}

func TestNtripChannelBeanCloseIsIdempotentAndSendSafe(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	bean := NewNtripChannelBean("MOUNT", clientConn, "")
	if cap(bean.send) != defaultNtripSendQueueSize {
		t.Fatalf("unexpected send queue size: %d", cap(bean.send))
	}
	bean.Send([]byte("data"))

	bean.Close()
	bean.Close()
	bean.Send([]byte("ignored"))
	bean.SendLoss([]byte("ignored"))
}

func TestNtripChannelBeanSendLossClonesPayload(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	bean := NewNtripChannelBean("MOUNT", clientConn, "")
	defer bean.Close()

	payload := []byte("first")
	bean.SendLoss(payload)
	copy(payload, []byte("zzzzz"))

	if err := serverConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline returned error: %v", err)
	}
	buf := make([]byte, len(payload))
	n, err := serverConn.Read(buf)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if string(buf[:n]) != "first" {
		t.Fatalf("expected cloned payload, got %q", string(buf[:n]))
	}
}

func TestSafeNtripMapCloseAllClosesConnectionsAndClearsStats(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	m := NewSafeNtripMap()
	bean := NewNtripChannelBean("MOUNT_CLOSE_ALL", clientConn, "")
	m.Set("key", bean)
	mutex.Lock()
	mountBytes["MOUNT_CLOSE_ALL"] = 256
	mutex.Unlock()

	m.CloseAll()

	if got := m.Get("key"); got != nil {
		t.Fatalf("expected map entry to be removed, got %#v", got)
	}
	mutex.Lock()
	_, ok := mountBytes["MOUNT_CLOSE_ALL"]
	mutex.Unlock()
	if ok {
		t.Fatal("expected mount stats to be deleted")
	}
	if _, err := clientConn.Write([]byte("x")); err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestSafeNtripMapDeleteReturnsRemovedBean(t *testing.T) {
	m := NewSafeNtripMap()
	bean := &NtripChannelBean{mount: "MOUNT"}
	m.Set("key", bean)

	got := m.Delete("key")

	if got != bean {
		t.Fatalf("expected deleted bean, got %#v", got)
	}
	if m.Get("key") != nil {
		t.Fatal("expected key to be removed")
	}
}

func TestSafeNtripMapMountIndexUpdatesOnSetAndDelete(t *testing.T) {
	m := NewSafeNtripMap()
	first := &NtripChannelBean{mount: "MOUNT_A"}
	second := &NtripChannelBean{mount: "MOUNT_B"}

	m.Set("key", first)
	m.Set("key", second)

	if got := m.GetByMount("MOUNT_A"); len(got) != 0 {
		t.Fatalf("expected old mount index to be empty, got %#v", got)
	}
	if got := m.GetByMount("MOUNT_B"); len(got) != 1 || got[0] != second {
		t.Fatalf("expected new mount index to contain second bean, got %#v", got)
	}

	m.Delete("key")
	if got := m.GetByMount("MOUNT_B"); len(got) != 0 {
		t.Fatalf("expected mount index to be empty after delete, got %#v", got)
	}
}

func TestSafeNtripMapForEachByMount(t *testing.T) {
	m := NewSafeNtripMap()
	m.Set("one", &NtripChannelBean{mount: "MOUNT_A"})
	m.Set("two", &NtripChannelBean{mount: "MOUNT_A"})
	m.Set("three", &NtripChannelBean{mount: "MOUNT_B"})

	count := 0
	m.ForEachByMount("MOUNT_A", func(bean *NtripChannelBean) {
		count++
	})

	if count != 2 {
		t.Fatalf("expected 2 beans for mount, got %d", count)
	}
}

func TestDeleteMountBytesRemovesEntry(t *testing.T) {
	mutex.Lock()
	mountBytes["MOUNT_TO_DELETE"] = 1024
	mutex.Unlock()

	deleteMountBytes("MOUNT_TO_DELETE")

	mutex.Lock()
	_, ok := mountBytes["MOUNT_TO_DELETE"]
	mutex.Unlock()
	if ok {
		t.Fatal("expected mount byte stats to be deleted")
	}
}
