package ipc

import (
	"context"
	"testing"
	"time"
)

func TestUDSClient_FlushDrainsSpool(t *testing.T) {
	dir := t.TempDir()
	sock := dir + "/f.sock"

	rec := &recorder{}
	srv, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()

	c, err := NewClient(sock, ClientConfig{SpoolPath: dir + "/f.spool", Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.Start()
	defer c.Stop()

	if !waitFor(t, time.Second, c.Connected) {
		t.Fatalf("client did not connect")
	}

	// Spool a burst; the echo/recorder server keeps reading, so the spool drains.
	for i := 0; i < 50; i++ {
		if err := c.Send(NewMessage(TypeRaw, []byte("x"), NextSeq())); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if st := c.Stats(); st.SpoolBytes != 0 {
		t.Errorf("spool not drained: %d bytes", st.SpoolBytes)
	}
}

func TestUDSClient_FlushTimeoutWhenDownstreamDown(t *testing.T) {
	dir := t.TempDir()
	sock := dir + "/fd.sock" // nothing listening
	c, err := NewClient(sock, ClientConfig{SpoolPath: dir + "/fd.spool", Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.Start()
	defer c.Stop()

	if err := c.Send(NewMessage(TypeRaw, []byte("x"), NextSeq())); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); err == nil {
		t.Error("Flush should time out when downstream is down")
	}
}

// TestRawFrame_GeneratedGetters covers the protoc-gen-go accessors for the
// regenerated RawFrame message (added in Phase 4) so the generated code path
// stays covered.
func TestRawFrame_GeneratedGetters(t *testing.T) {
	rf := &RawFrame{
		Source:      "BINANCE",
		Payload:     []byte("hello"),
		ReceivedAt:  123,
		SequenceNum: 7,
	}
	if rf.GetSource() != "BINANCE" {
		t.Errorf("GetSource: %q", rf.GetSource())
	}
	if string(rf.GetPayload()) != "hello" {
		t.Errorf("GetPayload: %q", rf.GetPayload())
	}
	if rf.GetReceivedAt() != 123 {
		t.Errorf("GetReceivedAt: %d", rf.GetReceivedAt())
	}
	if rf.GetSequenceNum() != 7 {
		t.Errorf("GetSequenceNum: %d", rf.GetSequenceNum())
	}
	rf.Reset()
	if rf.GetSource() != "" || rf.GetReceivedAt() != 0 {
		t.Errorf("Reset did not clear: %+v", rf)
	}
	_ = (&RawFrame{}).String()
	(&RawFrame{}).ProtoMessage()
	_, _ = (&RawFrame{}).Descriptor()
	_ = (&RawFrame{}).ProtoReflect()
	// nil-receiver getters return zero values (never panic).
	var nilRF *RawFrame
	if nilRF.GetSource() != "" || nilRF.GetSequenceNum() != 0 {
		t.Error("nil RawFrame getter panicked or non-zero")
	}
}
