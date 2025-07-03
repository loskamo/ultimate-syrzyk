#!/usr/bin/env python3
"""
calc_advanced_metrics_mp.py  –  процесна паралелізація (32 ядра)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import fasttext
from hunspell import HunSpell
from wordfreq import word_frequency

# ─── конфіг ─────────────────────────────────────────────
PARQUET_IN  = Path("../data/corp.parquet")
PARQUET_OUT = PARQUET_IN
FASTTEXT_MODEL = "../models/lid.176.ftz"
DICT_DIR = "/usr/share/hunspell"          # онови, якщо треба
N_JOBS   = 128                             # або -1 = os.cpu_count()
CHUNK_ROWS = 50000                     # 5 млн / 32 ≈ 156 k
# ────────────────────────────────────────────────────────

WORD_RE = re.compile(r"\b\w+\b", re.UNICODE)
MIX_RE  = re.compile(r"(?=.*[A-Za-z])(?=.*[А-яЁёЇїІіЄєҐґ])")

def init_worker():
    """
    Ініціалізується один раз у кожному процесі-воркері.
    Створюємо глобальні об’єкти fastText + Hunspell.
    """
    global _ft, _hun_uk, _hun_ru
    _ft = fasttext.load_model(FASTTEXT_MODEL)
    _hun_uk = HunSpell(f"{DICT_DIR}/uk_UA.dic", f"{DICT_DIR}/uk_UA.aff")
    _hun_ru = HunSpell(f"{DICT_DIR}/ru_RU.dic", f"{DICT_DIR}/ru_RU.aff")

def is_borrowed(word: str) -> bool:
    if any(ch.isascii() and ch.isalpha() for ch in word):
        return True
    en = word_frequency(word, "en")
    uk = word_frequency(word, "uk")
    ru = word_frequency(word, "ru")
    return en > max(uk, ru) * 3 and en > 2e-8

def is_orthomix(word: str) -> bool:
    if MIX_RE.search(word):
        return True
    return not _hun_uk.spell(word) and not _hun_ru.spell(word)

def process_block(texts):
    """Обчислити метрики для блоку рядків (список str)."""
    lb_cnt  = np.zeros(len(texts), dtype=np.uint16)
    lb_rat  = np.zeros(len(texts), dtype=np.float32)
    om_cnt  = np.zeros(len(texts), dtype=np.uint16)
    om_rat  = np.zeros(len(texts), dtype=np.float32)

    for i, txt in enumerate(texts):
        words = WORD_RE.findall(txt)
        if not words:
            continue
        total = len(words)
        bc = sum(is_borrowed(w.lower()) for w in words)
        oc = sum(is_orthomix(w)        for w in words)
        lb_cnt[i] = bc
        lb_rat[i] = bc / total
        om_cnt[i] = oc
        om_rat[i] = oc / total
    return lb_cnt, lb_rat, om_cnt, om_rat

# ─── main ───────────────────────────────────────────────
df = pd.read_parquet(PARQUET_IN)
blocks = [
    df["raw_comment"].iloc[i : i + CHUNK_ROWS].tolist()
    for i in range(0, len(df), CHUNK_ROWS)
]

print(f"Chunks: {len(blocks)}, rows/chunk ≈ {CHUNK_ROWS}")
results = []

with ProcessPoolExecutor(max_workers=N_JOBS, initializer=init_worker) as pool:
    for res in tqdm(pool.map(process_block, blocks), total=len(blocks)):
        results.append(res)

# зшили назад
lb_cnt_all  = np.concatenate([r[0] for r in results])
lb_rat_all  = np.concatenate([r[1] for r in results])
om_cnt_all  = np.concatenate([r[2] for r in results])
om_rat_all  = np.concatenate([r[3] for r in results])

df["lexical_borrowing_count"]  = lb_cnt_all
df["lexical_borrowing_ratio"]  = lb_rat_all
df["orthographic_mixing_count"] = om_cnt_all
df["orthographic_mixing_ratio"] = om_rat_all

df.to_parquet(PARQUET_OUT, engine="pyarrow", index=False)
print("✓ saved", PARQUET_OUT)
