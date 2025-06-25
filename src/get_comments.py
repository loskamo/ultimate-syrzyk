import json
import subprocess
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

URLS_FILE = "../data/urls.json"
OUTPUT_DIR = "../data/comments"

MAX_WORKERS = 128
RETRIES = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(URLS_FILE, "r", encoding="utf-8") as f:
    video_urls = json.load(f)

def download_comments(url):
    video_id = url.split("v=")[-1]
    output_file = os.path.join(OUTPUT_DIR, f"{video_id}.json")

    if os.path.exists(output_file):
        return f"[skip] {video_id}"

    for attempt in range(1, RETRIES + 1):
        try:
            subprocess.run([
                "youtube-comment-downloader",
                "--url", url,
                "--output", output_file
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return f"[✓] {video_id}"
        except subprocess.CalledProcessError:
            time.sleep(0.5)

    return f"[X] Failed {video_id}"

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_comments, url) for url in video_urls]
    for future in as_completed(futures):
        print(future.result())
