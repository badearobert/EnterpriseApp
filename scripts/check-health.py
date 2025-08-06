import sys
import requests

url = "http://localhost:5000/health"

try:
    response = requests.get(url, timeout=5)
    if response.status_code == 200:
        print("Health check successful.")
        sys.exit(0)
    else:
        print(f"Health check failed with status code: {response.status_code}")
        sys.exit(1)
except requests.RequestException as e:
    print(f"Health check failed: {e}")
    sys.exit(1)
