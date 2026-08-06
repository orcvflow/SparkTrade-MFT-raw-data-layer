#!/usr/bin/env python3
import http.client
import sys

with open('/tmp/init_schema.dos', 'r') as f:
    script = f.read()

conn = http.client.HTTPConnection('localhost', 8848, timeout=60)
try:
    conn.request('POST', '/', script.encode('utf-8'), {'Content-Type': 'text/plain'})
    response = conn.getresponse()
    body = response.read().decode('utf-8', errors='replace')
    
    print(body)
    sys.exit(0 if response.status == 200 else 1)
    
except Exception as e:
    print(f"HTTP error: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    conn.close()
