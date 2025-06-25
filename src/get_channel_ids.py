import json
import requests
import time
from pathlib import Path

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
API_KEY_FILE = "../youtube_api_key.txt"
CHANNELS_FILE = "../data/youtube_channels_list.json"
IDS_FILE = "../data/channel_ids.json"

key_path = Path(API_KEY_FILE)
if key_path.exists():
    API_KEY = key_path.read_text(encoding="utf-8").strip()
else:
    raise FileNotFoundError("Файл {} не знайдено. Спочатку запусти config.sh та вкажи API KEY".format(API_KEY_FILE))

channels_path = Path(CHANNELS_FILE)
if channels_path.exists():
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        channel_names = json.load(f)["channels"]
else:
    raise FileNotFoundError("Файл {} не знайдено. Спочатку запусти config.sh та вкажи перелік бажаних каналів".format(CHANNELS_FILE))

channel_id_map = {}

for name in channel_names:
    params = {
        "part": "snippet",
        "q": name,
        "type": "channel",
        "maxResults": 1,
        "key": API_KEY
    }
    resp = requests.get(SEARCH_URL, params=params)
    data = resp.json()

    if "items" in data and data["items"]:
        channel_id = data["items"][0]["snippet"]["channelId"]
        channel_id_map[name] = channel_id
        print(f"[+] {name} → {channel_id}")
    else:
        print(f"[!] Не знайдено: {name}")
        channel_id_map[name] = None

    time.sleep(0.2)

with open(IDS_FILE, "w", encoding="utf-8") as f:
    json.dump(channel_id_map, f, ensure_ascii=False, indent=2)
