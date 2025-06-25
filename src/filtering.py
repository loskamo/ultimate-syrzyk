import json

def length_filter(text, min_len=5):
    return len(text.strip()) >= min_len

def has_cyrillic(text):
    return any("а" <= ch.lower() <= "я" or ch.lower() == "ї" or ch.lower() == "є" or ch.lower() == "ґ" or ch.lower() == "і" for ch in text)

FILTERS = [
    length_filter,
    has_cyrillic,
]

with open("../data/comments_cleanup.json", "r", encoding="utf-8") as f:
    raw_comments = json.load(f)

filtered = []
for comment in raw_comments:
    if all(rule(comment) for rule in FILTERS):
        filtered.append(comment)

with open("../data/comments_filtered.json", "w", encoding="utf-8") as f:
    json.dump(filtered, f, ensure_ascii=False, indent=2)

print(f"[✓] Було: {len(raw_comments)} → Залишилось: {len(filtered)}")
