import pandas as pd
from pathlib import Path

IN_PATH  = Path("../data/corp.parquet")
OUT_PATH = IN_PATH

print("→ Завантаження датасету …")
df = pd.read_parquet(IN_PATH)
initial_rows = len(df)
print(f"Початково: {initial_rows:,} записів")

# ── Фільтр 1: lang_ru_score ≤ 0.8 ──────────────────────────────
df = df[df["lang_ru_score"] <= 0.8]
print(f"Після lang_ru_score ≤ 0.8: {len(df):,}")

# ── Фільтр 2: lang_uk_score ≥ 0.2 ──────────────────────────────
df = df[df["lang_uk_score"] >= 0.2]
print(f"Після lang_uk_score ≥ 0.2: {len(df):,}")

# ── Фільтр 3: dominant_language у {'ru', 'uk'} ────────────────
df = df[df["dominant_language"].isin({"ru", "uk"})]
print(f"Після dominant_language ∈ {{'ru','uk'}}: {len(df):,}")

# ── Фільтр 4: avg_segment_length > 10
df = df[df["avg_segment_length"] <= 10]
print(f"Після avg_segment_length ≤ 10: {len(df):,} рядків")

# ── Фільтр 5: видаляємо коментарі з 1 слова
df = df[df["raw_comment"].str.split().str.len() > 1]
print(f"Після видалення 1-словних: {len(df):,} рядків")


df = df[(
    (df["lexical_borrowing_count"]  <= 5) &
    (df["lexical_borrowing_ratio"]  <= 0.333)
)]

print(f"Після фільтра: {len(df):,}")

df = df[(
    (df["orthographic_mixing_count"]  <= 7) &
    (df["orthographic_mixing_ratio"]  <= 0.80)
)]
print(f"Після фільтру  (count ≤ 7 та ratio ≤ 0.80): {len(df):,} рядків")

df = df[df["switching_freq"] <= 44]
print(f"after  filter: {len(df):,} rows")

mask_keep = ~((df["lang_uk_score"] > 0.99) & (df["lang_ru_score"] < 0.01))
df = df[mask_keep]

# ── Показуємо фінальний розподіл dominant_language ────────────
print("\n=== Розподіл dominant_language (після всіх фільтрів) ===")
print(df["dominant_language"].value_counts(dropna=False))

# ─── збереження ───
df.to_parquet(OUT_PATH, index=False)
print(f"\n✅ Відфільтрований корпус збережено → {OUT_PATH}")
