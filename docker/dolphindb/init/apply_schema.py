#!/usr/bin/env python3
import http.client
import json
import sys

# Read schema
with open('/tmp/init_schema.dos', 'r') as f:
    script = f.read()

# DolphinDB JSON API format
payload = {
    "sessionID": "",
    "functionName": "executeCode",
    "params": [{
        "name": "script",
        "form": "scalar",
        "type": "string",
        "value": script
    }]
}

conn = http.client.HTTPConnection('localhost', 8848, timeout=60)
try:
    body = json.dumps(payload).encode('utf-8')
    conn.request('POST', '/', body, {'Content-Type': 'application/json'})
    response = conn.getresponse()
    result = json.loads(response.read().decode('utf-8'))
    
    print(f"Status: {response.status}")
    print(f"Result Code: {result.get('resultCode', 'N/A')}")
    print(f"Message: {result.get('msg', 'N/A')}")
    
    # Print object if exists (execution output)
    if 'object' in result and result['object']:
        print("\nOutput:")
        for item in result['object']:
            print(item)
    
    sys.exit(0 if result.get('resultCode') == '0' else 1)
    
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
finally:
    conn.close()
