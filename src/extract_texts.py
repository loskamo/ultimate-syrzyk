import glob
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

COMMENTS_DIR = "../data/comments"
OUTPUT_FILE = "../data/comments.json"
MAX_WORKERS = 1

total_lines = 0
total_comments_extracted = 0

def extract_text_fields(path):
    local_total = 0
    texts = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
            local_total = content.count('\n') + 1  # рядки ≈ кількість json-об'єктів
        matches = re.findall(r'"text"\s*:\s*"((?:[^"\\]|\\.)*)"', content)
        return (local_total, matches)
    except Exception as e:
        print(f"[!] File error in {path}: {e}")
        return (local_total, [])

comment_files = glob.glob(f"{COMMENTS_DIR}/*.json")
all_texts = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(extract_text_fields, path) for path in comment_files]
    for future in as_completed(futures):
        lines, texts = future.result()
        total_lines += lines
        total_comments_extracted += len(texts)
        all_texts.extend(texts)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_texts, f, ensure_ascii=False, indent=2)

print(f"[✓] Збережено {len(all_texts)} коментарів у {OUTPUT_FILE}")
print(f"[ℹ️] Рядків у всіх файлах разом: {total_lines}")
print(f"[ℹ️] Успішно витягнуто коментарів: {total_comments_extracted}")
print(f"[ℹ️] Втрачено або пошкоджено: {total_lines - total_comments_extracted}")
