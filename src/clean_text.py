import re
import json

def remove_urls(text: str) -> str:
    # Видаляє все, що виглядає як http(s) або www, або домени
    url_pattern = re.compile(
        r"(https?://\S+|www\.\S+|\S+\.(com|ua|net|org|tv|info|ru|su|dev|top|me|io|ly|site)\b\S*)",
        re.IGNORECASE
    )
    return url_pattern.sub('', text).strip()

def clean_text(text: str) -> str:
    text = remove_urls(text)
    
    text = re.sub(r"[^\w\s.,!?\"'’”“():;@#№\-\n]", '', text, flags=re.UNICODE)

    allowed = re.compile(r"[^\u0400-\u04FF\u0500-\u052F\u2DE0-\u2DFF\uA640-\uA69F\u0030-\u0039a-zA-Z\s.,!?\"'’”“():;@#№\-\n]", flags=re.UNICODE)
    text = allowed.sub('', text)

    text = re.sub(r'\s+', ' ', text).strip()
    return text

with open("../data/comments.json", "r", encoding="utf-8") as f:
    raw_comments = json.load(f)

cleaned_comments = [clean_text(text) for text in raw_comments if text.strip()]

with open("../data/comments_cleanup.json", "w", encoding="utf-8") as f:
    json.dump(cleaned_comments, f, ensure_ascii=False, indent=2)

print(f"[✓] Очищено: {len(cleaned_comments)} коментарів з {len(raw_comments)}")
