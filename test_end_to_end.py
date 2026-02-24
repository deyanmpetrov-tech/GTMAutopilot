import requests
import json
import time

files = {'gtm_file': open('dummy_test_container.json', 'rb')}
data = {'target_url': 'https://kids.advance-edu.org/#offer', 'gemini_key': 'testing'}
res = requests.post('http://localhost:8000/api/analyze', files=files, data=data)
task_id = res.json()['task_id']

while True:
    time.sleep(2)
    s = requests.get(f'http://localhost:8000/api/status/{task_id}').json()
    if s['status'] in ['review_required', 'error']:
        print("Status:", s['status'])
        print("Error:", s.get('error'))
        for line in s.get('logs', []):
            print(line)
        break
