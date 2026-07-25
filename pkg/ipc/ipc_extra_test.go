package ipc

import (
	"net"
	"testing"
	"time"
)

// TestIPCMessage_GeneratedGetters exercises the protoc-gen-go accessors so the
// generated code path is covered (NewMessage builds the message; the getters
// read it back).
func TestIPCMessage_GeneratedGetters(t *testing.T) {
	m := NewMessage("canonical", []byte("payload"), 5)
	if got := m.GetType(); got != "canonical" {
		t.Errorf("GetType=%q", got)
	}
	if got := m.GetPayload(); string(got) != "payload" {
		t.Errorf("GetPayload=%q", got)
	}
	if got := m.GetSeq(); got != 5 {
		t.Errorf("GetSeq=%d", got)
	}
	// Marker / introspection methods (no-op but exercise generated code).
	m.ProtoMessage()
	_ = m.String()
	_, _ = m.Descriptor()
	_ = m.ProtoReflect()
	m.Reset()
	// After Reset, the message is empty.
	if m.GetType() != "" || m.GetSeq() != 0 {
		t.Errorf("Reset did not clear: type=%q seq=%d", m.GetType(), m.GetSeq())
	}
}

func TestNextSeq(t *testing.T) {
	a := NextSeq()
	b := NextSeq()
	if b <= a {
		t.Errorf("NextSeq not monotonic: %d then %d", a, b)
	}
}

func TestServer_Path(t *testing.T) {
	sock := tmpSock(t)
	srv, err := Listen(sock, func(*IPCMessage) *IPCMessage { return nil })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()
	if got := srv.Path(); got != sock {
		t.Errorf("Path=%q want %q", got, sock)
	}
}

// TestUDSClient_DrainWriteError deterministically triggers a write failure
// during a spool drain: a raw server closes each accepted conn right after one
// read, so the client's subsequent writes fail → addError + breakConn +
// reconnect. This covers the client's error-recording and reconnect-on-write-
// failure paths.
func TestUDSClient_DrainWriteError(t *testing.T) {
	sock := tmpSock(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				_, _ = c.Read(buf) // consume a little, then close → breaks writes
			}(conn)
		}
	}()

	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	if !waitFor(t, 3*time.Second, client.Connected) {
		t.Fatal("client did not connect")
	}
	for i := 0; i < 20; i++ {
		_ = client.Send(NewMessage("raw", []byte("x"), uint64(i)))
	}

	if !waitFor(t, 3*time.Second, func() bool {
		st := client.Stats()
		return len(st.Errors) >= 1 || st.Reconnects >= 1
	}) {
		t.Errorf("expected a recorded write error or reconnect: %+v", client.Stats())
	}
}

// TestUDSClient_LosslessAcrossReconnects sends a burst, breaks the downstream,
// sends another burst (spooled while down), revives, and confirms every
// message arrived — no loss across the outage window.
func TestUDSClient_LosslessAcrossReconnects(t *testing.T) {
	sock := tmpSock(t)
	rec := &recorder{}

	srv, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	if !waitFor(t, 3*time.Second, client.Connected) {
		t.Fatal("client did not connect")
	}

	// First burst (delivered live).
	var seq uint64
	for i := 0; i < 5; i++ {
		if err := client.Send(NewMessage("canonical", []byte("a"), seq)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		seq++
	}
	if !waitFor(t, 3*time.Second, func() bool { return rec.count() >= 5 }) {
		t.Fatalf("first burst not delivered: %d", rec.count())
	}

	// Break the downstream and send a second burst (spooled while down).
	if err := srv.Stop(); err != nil {
		t.Fatalf("srv.Stop: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the client detect the drop
	for i := 0; i < 5; i++ {
		if err := client.Send(NewMessage("canonical", []byte("b"), seq)); err != nil {
			t.Fatalf("Send while down: %v", err)
		}
		seq++
	}

	// Revive.
	srv2, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen revive: %v", err)
	}
	defer srv2.Stop()

	if !waitFor(t, 5*time.Second, func() bool { return rec.count() >= 10 }) {
		t.Fatalf("lossless FAILED: delivered %d of 10", rec.count())
	}
}

