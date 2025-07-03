import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.float_format", '{:,.3f}'.format)

# Шлях до датасету
PARQUET_PATH = '../data/corp.parquet'

# Завантаження
print("→ Завантажую датасет …")
df = pd.read_parquet(PARQUET_PATH)

# Очистка dominant_language
print("→ Нормалізую dominant_language …")
df['dominant_language'] = df['dominant_language'].apply(lambda x: x if x in {'uk', 'ru', 'en'} else 'other')

# Метрики
metric_cols = [
    'lang_uk_score',
    'lang_ru_score',
    'switching_freq',
    'avg_segment_length',
    'embedded_lang_percent',
    'lexical_diversity',
    'lexical_borrowing_count',
    'lexical_borrowing_ratio',
    'orthographic_mixing_count',
    'orthographic_mixing_ratio',
    'language_entropy_index',
    'max_continuous_lang_sequence',
    'semantic_distance_ua',
    'semantic_distance_ru'
]

# Зведена статистика
print("\n=== Зведена статистика по метриках ===\n")
summary_df = df[metric_cols].describe(percentiles=[.01, .05, .25, .5, .75, .95, .99])
print(summary_df)

# Зберігаємо у CSV
summary_df.to_csv("../data/metric_summary.csv")
print("\nЗбережено як ../data/metric_summary.csv")

# Статистика по dominant_language
print("\n=== Частоти dominant_language ===\n")
print(df['dominant_language'].value_counts(normalize=True).round(4))

# Побудова графіків
print("\n→ Побудова графіків …")
os.makedirs("../img", exist_ok=True)
for col in metric_cols:
    plt.figure(figsize=(8, 4))
    sns.histplot(df[col], kde=False, bins=50)
    plt.title(f"Розподіл: {col}")
    plt.xlabel(col)
    plt.ylabel("Кількість")
    plt.tight_layout()
    plt.savefig(f"../img/{col}.png")
    plt.close()

print("\nГрафіки збережено в ../img/, статистику — у ../data/metric_summary.csv")
