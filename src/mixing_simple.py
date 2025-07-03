#!/usr/bin/env python3
"""
calculate_simple_metrics_parallel.py  (rev3)

Паралельне обчислення простих метрик мовного змішування.
Виправлена залежність: тепер не використовуємо joblib.tqdm, а
звичайний tqdm — сумісно з будь-якою версією joblib (<1.4).

Вимоги:
    pip install pandas pyarrow fasttext tqdm regex joblib
"""

import pandas as pd
import re
import fasttext
from pathlib import Path
from joblib import Parallel, delayed
from tqdm import tqdm

PARQUET_INPUT  = Path("../data/corp.parquet")
PARQUET_OUTPUT = PARQUET_INPUT
FASTTEXT_MODEL = Path("../models/lid.176.ftz")
N_JOBS         = -1  # всі ядра

_ft_model = None
word_re = re.compile(r"\b\w+\b", re.UNICODE)

def _init_model():
    global _ft_model
    if _ft_model is None:
        _ft_model = fasttext.load_model(str(FASTTEXT_MODEL))

def compute_metrics(text: str):
    _init_model()
    words = word_re.findall(text)
    if not words:
        return 0, 0.0, 0.0, 'und', 0.0

    langs = [_ft_model.predict(w)[0][0].replace("__label__", "") for w in words]

    switches = sum(langs[i] != langs[i-1] for i in range(1, len(langs)))

    segs, cur, cnt = [], langs[0], 1
    for lg in langs[1:]:
        if lg == cur:
            cnt += 1
        else:
            segs.append(cnt)
            cur, cnt = lg, 1
    segs.append(cnt)
    avg_seg_len = sum(segs) / len(segs)

    vc = pd.Series(langs).value_counts()
    dominant = vc.idxmax()
    embed_pct = (len(words) - vc.max()) / len(words)

    lex_div = len(set(words)) / len(words)

    return switches, avg_seg_len, embed_pct, dominant, lex_div

def main():
    print("→ Loading Parquet …")
    df = pd.read_parquet(PARQUET_INPUT)
    total = len(df)
    print(f"  {total:,} comments loaded")

    print("→ Computing metrics in parallel …")
    results = Parallel(n_jobs=N_JOBS, backend="loky", prefer="processes")(
        delayed(compute_metrics)(txt) for txt in tqdm(df["raw_comment"], total=total)
    )

    cols = ["switching_freq", "avg_segment_length", "embedded_lang_percent", "dominant_language", "lexical_diversity"]
    for col, values in zip(cols, zip(*results)):
        df[col] = list(values)

    print("→ Writing Parquet …")
    df.to_parquet(PARQUET_OUTPUT, engine="pyarrow", index=False)
    print(f"✅ Saved to {PARQUET_OUTPUT}")

if __name__ == "__main__":
    main()
