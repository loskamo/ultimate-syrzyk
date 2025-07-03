from pathlib import Path
import time, re
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm

# ────────── файли ──────────
CORP_PATH   = Path("../data/corp.parquet")
# ──────────────────────────

DEVICE   = "cuda" if torch.cuda.is_available() else "cpu"
MODEL_ID = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
BATCH    = 128            # ↑/↓ залежно від VRAM

UA_REF = "Це речення українською мовою."
RU_REF = "Это предложение на русском языке."

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ────────── main ──────────
def main():
    pf     = pq.ParquetFile(CORP_PATH)
    n_rows = pf.metadata.num_rows
    log(f"rows: {n_rows:,} | device: {DEVICE}")

    # ▸ модель SBERT
    sbert = SentenceTransformer(MODEL_ID, device=DEVICE)
    emb_ua = sbert.encode(UA_REF, convert_to_tensor=True, device=DEVICE)
    emb_ru = sbert.encode(RU_REF, convert_to_tensor=True, device=DEVICE)

    out_tables = []
    bar = tqdm(total=n_rows, unit="rows", desc="distance UA/RU")

    # ——— обхід row-group’ів
    for rg in range(pf.num_row_groups):
        tbl   = pf.read_row_group(rg, columns=["raw_comment"])
        texts = tbl.column("raw_comment").to_pylist()

        d_ua, d_ru = [], []

        # батч-інференс
        for i in range(0, len(texts), BATCH):
            batch = texts[i : i + BATCH]
            emb   = sbert.encode(batch,
                                 convert_to_tensor=True,
                                 device=DEVICE,
                                 batch_size=BATCH,
                                 show_progress_bar=False)
            sim_ua = util.cos_sim(emb, emb_ua).squeeze()
            sim_ru = util.cos_sim(emb, emb_ru).squeeze()
            d_ua.extend((1 - sim_ua).cpu().tolist())
            d_ru.extend((1 - sim_ru).cpu().tolist())

            bar.update(len(batch))

        # додаємо колонки
        tbl = tbl.append_column("semantic_distance_ua",
                                pa.array(d_ua, type=pa.float32()))
        tbl = tbl.append_column("semantic_distance_ru",
                                pa.array(d_ru, type=pa.float32()))
        out_tables.append(tbl)

    bar.close()

    # ▸ запис назад
    pq.write_table(pa.concat_tables(out_tables), CORP_PATH)
    log(f"✅ overwritten {CORP_PATH.name}  (додано 2 стовпчики)")

if __name__ == "__main__":
    main()
