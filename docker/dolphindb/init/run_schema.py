#!/usr/bin/env python3
import http.client
import sys

# Read schema file
with open('/tmp/init_schema.dos', 'r') as f:
    script = f.read()

# Send to DolphinDB HTTP API
conn = http.client.HTTPConnection('localhost', 8848, timeout=30)
try:
    conn.request('POST', '/', script.encode('utf-8'), {'Content-Type': 'text/plain'})
    response = conn.getresponse()
    result = response.read().decode('utf-8')
    print(result)
    sys.exit(0 if response.status == 200 else 1)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
finally:
    conn.close()
