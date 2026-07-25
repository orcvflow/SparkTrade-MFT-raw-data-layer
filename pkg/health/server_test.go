package health

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func getBody(t *testing.T, url string, wantStatus int) []byte {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d, want %d", url, resp.StatusCode, wantStatus)
	}
	body, _ := io.ReadAll(resp.Body)
	return body
}

func TestServer_HealthReadyLiveMetrics(t *testing.T) {
	var calls atomic.Int32
	srv := NewServer("testproc", 0, func() Snapshot {
		calls.Add(1)
		return Snapshot{Status: "ok", Component: map[string]int{"msgs": 42}}
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	port := srv.Port()
	if port == 0 {
		t.Fatal("no port")
	}
	base := "http://127.0.0.1:" + itoa(port)

	// /health → 200, component present, snapshot called.
	b := getBody(t, base+"/health", 200)
	var hc map[string]any
	if err := json.Unmarshal(b, &hc); err != nil {
		t.Fatalf("unmarshal health: %v\n%s", err, b)
	}
	if hc["status"] != "ok" {
		t.Errorf("status=%v", hc["status"])
	}
	if hc["name"] != "testproc" {
		t.Errorf("name=%v", hc["name"])
	}
	comp, _ := hc["component"].(map[string]any)
	if comp == nil || comp["msgs"] != float64(42) {
		t.Errorf("component lost: %v", hc["component"])
	}
	if hc["memory"] == nil || hc["goroutines"] == nil {
		t.Error("memory/goroutines missing")
	}
	if calls.Load() < 1 {
		t.Error("snapshot callback not invoked")
	}

	// /ready → 200 ready:true
	getBody(t, base+"/ready", 200)
	// /live → 200 alive:true
	getBody(t, base+"/live", 200)
	// /metrics → 200 (prometheus text)
	getBody(t, base+"/metrics", 200)
}

func TestServer_DegradedReturns503(t *testing.T) {
	srv := NewServer("degraded", 0, func() Snapshot {
		return Snapshot{Status: "degraded"}
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	base := "http://127.0.0.1:" + itoa(srv.Port())
	getBody(t, base+"/health", 503)
}

func TestServer_SnapshotPanicRecovers(t *testing.T) {
	srv := NewServer("panicky", 0, func() Snapshot {
		panic("boom")
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	base := "http://127.0.0.1:" + itoa(srv.Port())
	// Must not crash the server; returns degraded 503.
	b := getBody(t, base+"/health", 503)
	var hc map[string]any
	_ = json.Unmarshal(b, &hc)
	if hc["status"] != "degraded" {
		t.Errorf("expected degraded after panic, got %v", hc["status"])
	}
	// Server still serves /live after the panic.
	getBody(t, base+"/live", 200)
}

func TestServer_NilSnapshot(t *testing.T) {
	srv := NewServer("noprobe", 0, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	base := "http://127.0.0.1:" + itoa(srv.Port())
	b := getBody(t, base+"/health", 200)
	var hc map[string]any
	_ = json.Unmarshal(b, &hc)
	if hc["status"] != "ok" {
		t.Errorf("nil snapshot should be ok, got %v", hc["status"])
	}
}

func TestServer_StopIdempotentAndDoubleStartBlocked(t *testing.T) {
	srv := NewServer("once", 0, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := srv.Start(); err == nil {
		t.Error("second Start should error")
	}
	if err := srv.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Errorf("second Stop should be idempotent, got %v", err)
	}
}

func TestServer_ReadyFalse(t *testing.T) {
	srv := NewServer("notready", 0, nil)
	srv.SetReady(func() bool { return false })
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	base := "http://127.0.0.1:" + itoa(srv.Port())
	getBody(t, base+"/ready", 503)
}

// itoa avoids strconv import bloat in the test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
