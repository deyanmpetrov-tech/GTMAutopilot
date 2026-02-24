import requests
import time
import sys

print("Sending POST request to /api/analyze...")
files = {'gtm_file': open('dummy_test_container.json', 'rb')}
data = {'target_url': 'https://kids.advance-edu.org/#offer', 'gemini_key': 'dummy_gemini_key_for_testing'}
resp = requests.post('http://localhost:8000/api/analyze', files=files, data=data)
if resp.status_code != 200:
    print(f"Error: {resp.text}")
    sys.exit(1)

task_id = resp.json()['task_id']
print(f"Task ID queued: {task_id}")

last_log_idx = 0
while True:
    time.sleep(2)
    s_resp = requests.get(f'http://localhost:8000/api/status/{task_id}')
    s_data = s_resp.json()
    logs = s_data.get('logs', [])
    for log in logs[last_log_idx:]:
        print(f"[UI STREAM] {log}")
    last_log_idx = len(logs)
    
    if s_data['status'] in ['review_required', 'error', 'completed']:
        print(f"\nFinal Status Reached: {s_data['status']}\n")
        break

