import json
import re
from pathlib import Path
from typing import Tuple, List

import pandas as pd
import fasttext
from tqdm import tqdm


# --------------------------------------------------------------------------- #
# Шляхи та параметри                                                          
# --------------------------------------------------------------------------- #
INPUT_JSON      = Path("../data/comments_v002.json")
OUTPUT_PARQUET  = Path("../data/corp.parquet")
FASTTEXT_MODEL  = Path("../models/lid.176.ftz")   # попередньо завантаж
MIN_LEN         = 3                               # мінімальна довжина рядка
# --------------------------------------------------------------------------- #

LETTER_RE = re.compile(r'[A-Za-z\u0400-\u04FF]')  # латиниця + кирилиця


def contains_letter(txt: str) -> bool:
    """True, якщо у рядку є хоча б одна літера."""
    return bool(LETTER_RE.search(txt))


def load_comments(path: Path) -> List[str]:
    """Читає JSON-масив коментарів."""
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("Очікую JSON-масив рядків.")
    return data


def language_scores(txt: str, ft_model) -> Tuple[float, float, float]:
    """
    Повертає (uk, ru, en) ймовірності лише за fastText.
    """
    labels, probs = ft_model.predict(txt, k=5)
    scores = {"uk": 0.0, "ru": 0.0, "en": 0.0}
    for lab, prob in zip(labels, probs):
        lang = lab.replace("__label__", "")
        if lang in scores:
            scores[lang] = prob
    return scores["uk"], scores["ru"], scores["en"]


def main() -> None:
    if not FASTTEXT_MODEL.exists():
        raise FileNotFoundError(
            f"Не знайдено fastText-модель {FASTTEXT_MODEL}. "
            "Скачай її командою в докстринзі."
        )

    print("→ Завантажую коментарі …")
    comments_raw = load_comments(INPUT_JSON)
    print(f"  Прочитано: {len(comments_raw):,}")

    print("→ Фільтрую короткі / безлітерні рядки …")
    comments = [
        c.strip() for c in comments_raw
        if len(c.strip()) >= MIN_LEN and contains_letter(c)
    ]
    print(f"  Залишилось після фільтру: {len(comments):,}")

    print("→ Завантажую fastText LID‐модель …")
    ft_model = fasttext.load_model(str(FASTTEXT_MODEL))

    print("→ Обчислюю ймовірності мов …")
    records = []
    for txt in tqdm(comments, unit=" коментар"):
        uk, ru, en = language_scores(txt, ft_model)
        records.append((txt, uk, ru, en))

    print("→ Записую Parquet …")
    df = pd.DataFrame(
        records,
        columns=["raw_comment", "lang_uk_score", "lang_ru_score", "lang_en_score"]
    )
    df.to_parquet(OUTPUT_PARQUET, engine="pyarrow", index=False)
    print(f"✓ Готово. Файл збережено у {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
