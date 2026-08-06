#!/usr/bin/env python3
# Verify DolphinDB tables via api-go (TCP binary protocol)
import subprocess
import json

# Use Go to query via api-go
script = """
package main
import (
	"context"
	"fmt"
	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer"
	"time"
)
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	db, err := api.NewDolphinDBClient(ctx, "localhost:8848", &dialer.BehaviorOptions{})
	if err != nil { fmt.Println("ERR:", err); return }
	defer db.Close()
	
	if err := db.Connect(); err != nil { fmt.Println("ERR:", err); return }
	if err := db.Login(&api.LoginRequest{UserID: "admin", Password: "123456"}); err != nil { fmt.Println("ERR:", err); return }
	
	// Check raw_events
	raw, err := db.Run(`select count(*) from loadTable("dfs://raw_data", "raw_events")`)
	if err != nil { fmt.Println("raw_events ERR:", err) } else { fmt.Println("raw_events count:", raw) }
	
	// Check canonical_events  
	canon, err := db.Run(`select count(*) from loadTable("dfs://raw_data", "canonical_events")`)
	if err != nil { fmt.Println("canonical_events ERR:", err) } else { fmt.Println("canonical_events count:", canon) }
}
"""

# Write temp Go file
with open('/tmp/verify_db.go', 'w') as f:
    f.write(script)

# Run
result = subprocess.run(['go', 'run', '/tmp/verify_db.go'], 
                       capture_output=True, text=True, cwd='/home/main/Desktop/raw-data-layer', timeout=15)
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)
print("Exit code:", result.returncode)
