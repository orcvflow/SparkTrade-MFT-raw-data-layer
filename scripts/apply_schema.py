#!/usr/bin/env python3
"""
Apply DolphinDB schema via HTTP API
Usage: python3 scripts/apply_schema.py
"""
import sys
import requests

def main():
    # Read schema file
    schema_file = 'docker/dolphindb/init/init_schema.dos'
    try:
        with open(schema_file, 'r') as f:
            schema_script = f.read()
    except FileNotFoundError:
        print(f"ERROR: {schema_file} not found")
        sys.exit(1)
    
    # Execute via DolphinDB HTTP API
    print("Applying DolphinDB schema...")
    print(f"Endpoint: http://localhost:8848/run")
    print(f"Script: {schema_file}")
    print("-" * 60)
    
    try:
        response = requests.post(
            'http://localhost:8848/run',
            data=schema_script,
            headers={'Content-Type': 'text/plain'},
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        print("-" * 60)
        print("Response:")
        print(response.text)
        
        if response.status_code == 200:
            print("\n✅ Schema applied successfully!")
            sys.exit(0)
        else:
            print(f"\n❌ Failed with status {response.status_code}")
            sys.exit(1)
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Cannot connect to DolphinDB (http://localhost:8848)")
        print("   Is DolphinDB container running?")
        print("   Check: docker ps | grep dolphindb")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
