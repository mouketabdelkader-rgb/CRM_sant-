import sqlite3
import pandas as pd
import numpy as np

# Config
PREV_DB = "crm_sante_previous.db"
DROP_PCT = 0.1  # 10% drop pour ~20k lignes supprimées, ~1.4k IDs nouveaux
SEED = 42  # Pour repro

print(f"Drop {DROP_PCT*100:.0f}% aléatoire de {PREV_DB} (seed {SEED})")

conn = sqlite3.connect(PREV_DB)
df_full = pd.read_sql("SELECT * FROM personnes_activites", conn)
conn.close()

print(f"Lignes initiales: {len(df_full)}")

# Drop aléatoire (sample sans remplacement pour supprimer)
np.random.seed(SEED)
drop_indices = np.random.choice(len(df_full), size=int(len(df_full) * DROP_PCT), replace=False)
df_drop = df_full.drop(drop_indices).reset_index(drop=True)

print(f"Lignes après drop: {len(df_drop)} (supprimé: {len(df_full) - len(df_drop)})")

# Réécrit en previous
conn = sqlite3.connect(PREV_DB)
df_drop.to_sql('personnes_activites', conn, if_exists='replace', index=False)
conn.close()

print("Drop terminé – Previous mis à jour avec trous simulés.")
