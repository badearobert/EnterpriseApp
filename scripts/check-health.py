import sys
import requests

ports = [5000, 5001, 5002]
all_ok = True

for port in ports:
    url = f"http://localhost:{port}/health"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print(f"{url}: Health check successful.")
        else:
            print(f"{url}: Health check failed with status code: {response.status_code}")
            all_ok = False
    except requests.RequestException as e:
        print(f"{url}: Health check failed: {e}")
        all_ok = False

if all_ok:
    sys.exit(0)
else:
    sys.exit(1)
