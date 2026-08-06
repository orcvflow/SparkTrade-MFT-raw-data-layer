#!/usr/bin/env python3
import http.client

conn = http.client.HTTPConnection('localhost', 8848, timeout=10)
try:
    conn.request('POST', '/', b'1+1', {'Content-Type': 'text/plain'})
    response = conn.getresponse()
    print(f"Status: {response.status}")
    print(f"Result: {response.read().decode('utf-8')}")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
