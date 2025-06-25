import json
import requests
import time
from pathlib import Path

API_KEY_FILE = "../youtube_api_key.txt"
API_URL = "https://www.googleapis.com/youtube/v3/search"
IDS_FILE = "../data/channel_ids.json"
URLS_FILE = "../data/urls.json"

key_path = Path(API_KEY_FILE)
if key_path.exists():
    API_KEY = key_path.read_text(encoding="utf-8").strip()
else:
    raise FileNotFoundError("Файл {} не знайдено. Спочатку запусти config.sh та вкажи API KEY".format(API_KEY_FILE))

ids_path = Path(IDS_FILE)
if ids_path.exists():
    with open(IDS_FILE, "r", encoding="utf-8") as f:
        channel_id_map = json.load(f)
else:
    raise FileNotFoundError("Файл {} не знайдено. Спочатку запусти config.sh та вкажи перелік бажаних каналів".format(IDS_FILE))

video_urls = []

for channel_name, channel_id in channel_id_map.items():
    if not channel_id:
        continue

    print(f"[+] Завантажую відео для каналу: {channel_name}")
    page_token = None

    while True:
        params = {
            "key": API_KEY,
            "channelId": channel_id,
            "part": "id",
            "order": "date",
            "maxResults": 50,
            "type": "video"
        }
        if page_token:
            params["pageToken"] = page_token

        response = requests.get(API_URL, params=params)
        data = response.json()

        for item in data.get("items", []):
            video_id = item["id"]["videoId"]
            url = f"https://www.youtube.com/watch?v={video_id}"
            video_urls.append(url)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        time.sleep(0.3)

with open(URLS_FILE, "w", encoding="utf-8") as f:
    json.dump(video_urls, f, ensure_ascii=False, indent=2)

print(f"[✓] Отримано {len(video_urls)} відео.")
