// Package storage — Addım F F2: LIVE DolphinDB integration test.
//
// Addım F migrated the production DolphinDBWriter transport from HTTP POST /run
// (rejected by v3: "Unsupport http request") to the official api-go native
// client. This test exercises the PRODUCTION path end-to-end against a live
// DolphinDB v3 container — i.e. the same NewDolphinDBWriter → Connect() →
// Write() → Flush() → runScript() code the storage process runs in production.
// A pass proves the transport migration is complete and the writer actually
// persists to DolphinDB.
//
// HONEST SCOPE: gated on LIVE_DOLPHINDB=1 (CI-safe; skips otherwise). Requires
// the dolphindb container running on :8848 (admin/123456):
//
//	docker run -d --name dolphindb -p 8848:8848 dolphindb/dolphindb:v3.00.2.1
//	LIVE_DOLPHINDB=1 go test ./pkg/storage/ -run TestIntegration_LiveDolphinDB -v -timeout 60s
//
// This is also the test that catches script-syntax regressions the permissive
// unit-test fake cannot (the fake does not validate DolphinDB syntax).
package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

func TestIntegration_LiveDolphinDB(t *testing.T) {
	if os.Getenv("LIVE_DOLPHINDB") == "" {
		t.Skip("set LIVE_DOLPHINDB=1 + run dolphindb container on :8848 (admin/123456)")
	}

	host := "localhost"
	if h := os.Getenv("DOLPHINDB_LIVE_HOST"); h != "" {
		host = h
	}
	port := 8848
	if p := os.Getenv("DOLPHINDB_LIVE_PORT"); p != "" {
		var n int
		if _, err := fmt.Sscanf(p, "%d", &n); err == nil && n > 0 {
			port = n
		}
	}

	const dbName = "dfs://raw_data"

	// 1. Production writer (apiGoRunner). Connect() does api-go TCP+login+
	//    ensureTables+replayWAL. Nil WAL is fine: this test drives Write/Flush
	//    manually and verifies via runScript; the lossless WAL path is covered
	//    by the unit tests.
	w := NewDolphinDBWriter(DolphinDBConfig{
		Host: host, Port: port, Username: "admin", Password: "123456",
		Database: dbName, BatchSize: 1000, BatchTimeout: time.Second,
	}, nil)

	if err := w.Connect(); err != nil {
		t.Skipf("connect (container down on %s:%d?): %v", host, port, err)
	}
	defer w.Stop()
	t.Logf("connected+logged in to %s:%d via api-go (production transport)", host, port)

	// 2. Idempotent: drop any prior db so the count assertion is exact on
	// repeated runs. Ignore "does not exist" errors. Uses the writer's OWN
	// runScript (production transport), not a side-channel client.
	if err := w.runScript(`try{ dropDatabase("` + dbName + `") }catch(ex){}`); err != nil {
		t.Fatalf("dropDatabase: %v", err)
	}
	// Recreate empty tables (Connect already ran ensureTables once; this recreates
	// after the drop so the count check below is exact).
	if err := w.ensureTables(); err != nil {
		t.Fatalf("ensureTables after drop: %v", err)
	}
	t.Logf("tables recreated (raw_events, canonical_events)")

	// 3. Build + write 10 canonical events via the PRODUCTION Write path
	//    (WAL nil → batch only) then Flush → writeBatch → buildInsertScript →
	//    runScript (api-go). BatchSize=1000 so the 10 events do not auto-flush;
	//    the explicit Flush drives the insert.
	events := make([]*canonicalizer.CanonicalEvent, 10)
	for i := range events {
		events[i] = &canonicalizer.CanonicalEvent{
			EventID:           fmt.Sprintf("live-f2-%d", i),
			Source:            "BINANCE",
			CanonicalSymbol:   "BTC/USD",
			ExchangeTimestamp: time.Now().UnixNano(),
			LocalHWTimestamp:  time.Now().UnixNano(),
			EventType:         "TRADE",
			Price:             50000.0 + float64(i),
			Size:              0.1,
			Side:              "BUY",
			RawPayload:        []byte(`{"e":"aggTrade","p":"50000","q":"0.1"}`),
			RawFormat:         "JSON",
		}
	}
	for i, ev := range events {
		if err := w.Write(ev); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	t.Logf("wrote %d events via production Write+Flush path", len(events))

	// 4. Read back via the production runScript path: throw if counts != 10
	//    (a thrown error surfaces as a runScript error with the count in the msg).
	check := func(table string) {
		script := fmt.Sprintf(
			`if((exec count(*) from loadTable("%s","%s")) != 10){ throw "%s count mismatch" }`,
			dbName, table, table)
		if err := w.runScript(script); err != nil {
			t.Fatalf("count check %s: %v", table, err)
		}
	}
	check("raw_events")
	check("canonical_events")
	t.Logf("PASS: raw_events==10, canonical_events==10 (read back via api-go)")

	// 5. Spot-check a value landed correctly: price 50000.0 must be among the
	//    canonical prices (a DFS HASH table has no guaranteed row order, so
	//    limit 1 would return an arbitrary row — use membership instead).
	verify := `if(!(50000.0 in exec price from loadTable("` + dbName + `","canonical_events"))){ throw "price 50000.0 not found" }`
	if err := w.runScript(verify); err != nil {
		t.Fatalf("price verify: %v", err)
	}
	t.Logf("PASS: canonical_events contains price 50000.0 (round-trip integrity)")
}
