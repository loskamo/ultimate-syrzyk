#!/usr/bin/env python3
"""
update_entropy_maxseq.py
   ▸ додає language_entropy_index та max_continuous_lang_sequence
   ▸ перезаписує corp.parquet
"""

from pathlib import Path
import os, re, time
import numpy as np
import pandas as pd
import fasttext
from joblib import Parallel, delayed, parallel
from tqdm import tqdm
from contextlib import contextmanager

# ---------- файли ----------
PARQUET_PATH = Path("../data/corp.parquet")
# ----------------------------

FASTTEXT_MODEL = "../models/lid.176.ftz"
N_JOBS   = 32
BATCH_SZ = 4096
WORD_RE  = re.compile(r"\b\w+\b", re.UNICODE)

# ---------- прогрес-joblib ----------
@contextmanager
def tqdm_joblib(tqdm_object):
    class TqdmBatchCallback(parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)
    old_cb = parallel.BatchCompletionCallBack
    parallel.BatchCompletionCallBack = TqdmBatchCallback
    try:
        yield
    finally:
        parallel.BatchCompletionCallBack = old_cb
        tqdm_object.close()
# ------------------------------------

# ---------- lazy fastText у воркері ----------
_ft = None
def ft_model():
    global _ft
    if _ft is None:
        _ft = fasttext.load_model(FASTTEXT_MODEL)
    return _ft
# ---------------------------------------------

def safe_lang(word: str) -> str:
    if re.fullmatch(r"[A-Za-z]+", word):
        return "en"
    if re.fullmatch(r"[А-Яа-яЁёЇїІіЄєҐґ]+", word):
        lab = ft_model().predict(word)[0][0].replace("__label__", "")
        return "ru" if lab == "ru" else "uk"
    return "other"

def metrics_for_text(txt: str):
    words = WORD_RE.findall(txt)
    if not words:
        return 0.0, 0

    langs = [safe_lang(w) for w in words]

    # ентропія
    _, cnt = np.unique(langs, return_counts=True)
    p = cnt / cnt.sum()
    ent = -(p * np.log2(p)).sum()

    # найдовший моно‐фрагмент
    longest = cur = 1
    for i in range(1, len(langs)):
        cur  = cur + 1 if langs[i] == langs[i - 1] else 1
        longest = max(longest, cur)

    return ent, longest

def main():
    print(f"[{time.strftime('%H:%M:%S')}] loading corp.parquet …")
    df = pd.read_parquet(PARQUET_PATH)
    n_rows = len(df)
    print(f"rows: {n_rows:,} | cpu jobs: {N_JOBS}")

    texts = df["raw_comment"].tolist()

    # --- паралель із прогресом ---
    with tqdm_joblib(tqdm(total=n_rows, unit="rows", desc="entropy/maxseq")):
        res = Parallel(
            n_jobs=N_JOBS,
            backend="loky",
            batch_size=BATCH_SZ,
            prefer="processes",
            verbose=0,
        )(delayed(metrics_for_text)(t) for t in texts)

    res = np.array(res)
    df["language_entropy_index"]       = res[:, 0]
    df["max_continuous_lang_sequence"] = res[:, 1]

    # ---------- перезапис ----------
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"[{time.strftime('%H:%M:%S')}] ✅ overwritten {PARQUET_PATH.name}")

if __name__ == "__main__":
    main()
