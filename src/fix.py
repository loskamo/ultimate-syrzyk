import pandas as pd

# Завантажуємо старий і новий датасети
df_old = pd.read_parquet("../data/corp_bak1.parquet")
df_new = pd.read_parquet("../data/corp.parquet")

# Видаляємо з нового датасету всі колонки, які вже є в старому (залишаємо тільки нові)
cols_to_add = df_new.columns.difference(df_old.columns)
df_new_filtered = df_new[cols_to_add]

# Додаємо нові колонки до старого датасету
df_merged = pd.concat([df_old, df_new_filtered], axis=1)

# Зберігаємо результат
df_merged.to_parquet("../data/corp_new.parquet", index=False)

# Перевірка
print("✅ Мерж успішний. Розмір:", df_merged.shape)
print("🔎 Колонки:", df_merged.columns.tolist())
