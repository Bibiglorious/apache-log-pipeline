 # STEP 1
# Fetch logs from Apache API with retry logic

import requests
import json

def fetch_logs_with_retry():
    
    api_url = "https://apache-api.onrender.com/logs"
    logs = []

    for attempt in range(3):
        response = requests.get(api_url)
        
        if response.status_code == 200:
            print("Successfully fetched logs from API")
            json_data = json.loads(response.text)
            logs = json_data["raw_logs"]
            break
        else:
            print(f"Failed to fetch logs from API. Status code: {response.status_code}")
            if attempt < 2:
                print("Retrying...")
            else:
                print("All attempts to fetch logs from API failed")
                
    print(f"Step 1 complete: {len(logs)} logs fetched")
    return logs

if __name__ == "__main__":
    logs = fetch_logs_with_retry()
    print(f"Total logs fetched: {len(logs)}")