import sqlite3
import pandas as pd
import os

# Config
CUR_DB = "crm_sante_current.db"
PREV_DB = "crm_sante_previous.db"
LIMIT_ROWS = 200000  # ~10% pour test
MIN_SIZE_MB = 1  # Seuil pour DB valide (évite vide)

def has_table(db_path):
    """Check si table 'personnes_activites' existe dans DB"""
    if not os.path.exists(db_path) or os.path.getsize(db_path) < MIN_SIZE_MB * 1024 * 1024:
        return False
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(personnes_activites)")
    table_exists = cursor.fetchall()
    conn.close()
    return bool(table_exists)

# Détermine source DB (priorité current valide, fallback previous valide)
source_db = None
if has_table(CUR_DB):
    source_db = CUR_DB
elif has_table(PREV_DB):
    source_db = PREV_DB
else:
    raise Exception(f"Aucune DB valide trouvée : {CUR_DB} (taille {os.path.getsize(CUR_DB)/ (1024*1024):.1f} Mo) ni {PREV_DB} (taille {os.path.getsize(PREV_DB)/ (1024*1024):.1f} Mo)")
print(f"Source DB valide: {source_db} (taille {os.path.getsize(source_db)/ (1024*1024):.1f} Mo)")

# Connexion et copie
conn_cur = sqlite3.connect(source_db)
df_subset = pd.read_sql(f"SELECT * FROM personnes_activites LIMIT {LIMIT_ROWS}", conn_cur)
conn_cur.close()

conn_prev = sqlite3.connect(PREV_DB)
df_subset.to_sql('personnes_activites', conn_prev, if_exists='replace', index=False)
conn_prev.close()

print(f"Copié {len(df_subset)} lignes dans {PREV_DB}")
