import sqlite3
import pandas as pd
from datetime import datetime
import os

PREV_DB = "crm_sante_previous.db"
CUR_DB = "crm_sante_current.db"
OUTPUT_CSV = f"nouveaux_pros_{datetime.now().strftime('%Y-%m')}.csv"

# Connexions + check previous
if not os.path.exists(PREV_DB):
    print(f"Previous DB absent ({PREV_DB}) – Assume 0 IDs précédents (tous nouveaux).")
    prev_ids = []
else:
    conn_prev = sqlite3.connect(PREV_DB)
    prev_ids = pd.read_sql('''
        SELECT DISTINCT "Identifiant PP" 
        FROM personnes_activites 
        WHERE "Libellé mode exercice" IN ('Lib,indép,artis,com', 'Salarié')
        AND "Identifiant PP" IS NOT NULL
    ''', conn_prev)['Identifiant PP'].dropna().unique()
    conn_prev.close()

conn_cur = sqlite3.connect(CUR_DB)
cur_ids = pd.read_sql('''
    SELECT DISTINCT "Identifiant PP" 
    FROM personnes_activites 
    WHERE "Libellé mode exercice" IN ('Lib,indép,artis,com', 'Salarié')
    AND "Identifiant PP" IS NOT NULL
''', conn_cur)['Identifiant PP'].dropna().unique()
conn_cur.close()

# Nouveaux IDs (set diff)
nouveaux_ids = set(cur_ids) - set(prev_ids)
print(f"IDs précédents: {len(prev_ids)} | IDs actuels: {len(cur_ids)} | Nouveaux détectés: {len(nouveaux_ids)}")

if len(nouveaux_ids) > 0:
    # Temp table en TEXT (anti-mismatch)
    conn_cur = sqlite3.connect(CUR_DB)
    cursor = conn_cur.cursor()
    cursor.execute('CREATE TEMP TABLE temp_nouveaux (id TEXT PRIMARY KEY)')
    cursor.executemany('INSERT OR IGNORE INTO temp_nouveaux VALUES (?)', [(str(id),) for id in nouveaux_ids])
    conn_cur.commit()
    
    # Query sur temp table
    df_nouveaux = pd.read_sql('''
        SELECT 
            "Identifiant PP", "Nom d'exercice", "Prénom d'exercice", "Libellé civilité d'exercice",
            "Libellé profession", "Libellé catégorie professionnelle", "Libellé mode exercice",
            "Identifiant technique de la structure", "Adresse e-mail (coord. structure)", 
            "Téléphone (coord. structure)", "Code postal (coord. structure)", 
            "Libellé commune (coord. structure)", "Libellé savoir-faire"
        FROM personnes_activites 
        WHERE "Identifiant PP" IN (SELECT id FROM temp_nouveaux)
        AND "Libellé mode exercice" IN ('Lib,indép,artis,com', 'Salarié')
    ''', conn_cur)
    
    # Nettoie
    cursor.execute('DROP TABLE temp_nouveaux')
    conn_cur.commit()
    conn_cur.close()
    
    df_nouveaux.to_csv(OUTPUT_CSV, index=False)
    print(f"Exporté {len(df_nouveaux)} nouveaux pros dans {OUTPUT_CSV}")
    
    print("\nTop 5 communes des nouveaux:")
    print(df_nouveaux['Libellé commune (coord. structure)'].value_counts().head())
    print("\nTop 5 professions des nouveaux:")
    print(df_nouveaux['Libellé profession'].value_counts().head())
    
    # Stat emails
    print(f"\nNouveaux pros avec email: {df_nouveaux['Adresse e-mail (coord. structure)'].notna().sum()}")
else:
    print("Aucun nouveau pro ce mois-ci.")

print("Détection terminée !")
