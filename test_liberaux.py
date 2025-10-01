# -*- coding: utf-8 -*-
import pandas as pd

CSV_IN = "nouveaux_pros_2025-09.csv"
CSV_OUT = "test_liberaux.csv"

df = pd.read_csv(CSV_IN, encoding='utf-8')
print("Colonnes détectées:", df.columns.tolist())  # Debug

col_mode = 'Libellé mode exercice'
col_lib = 'Lib,indép,artis,com'
liberaux = df[df[col_mode] == col_lib].copy()

cols_select = ['Identifiant PP', 'Nom d\'exercice', 'Prénom d\'exercice', 'Libellé profession', 
               'Identifiant technique de la structure', 'Adresse e-mail (coord. structure)', 
               'Code postal (coord. structure)', 'Libellé commune (coord. structure)']
liberaux = liberaux[cols_select]

print(f"Pros libéraux extraits: {len(liberaux)} / {len(df)}")

if len(liberaux) > 0:
    liberaux_top10 = liberaux.head(10)
    liberaux_top10.to_csv(CSV_OUT, index=False, encoding='utf-8')
    print(f"Top 10 exporté dans {CSV_OUT}")
    print(liberaux_top10[['Nom d\'exercice', 'Libellé profession', 'Identifiant technique de la structure']].to_string(index=False))
else:
    print("Aucun libéral trouvé – Vérif col mode.")
    print(df[col_mode].value_counts().head())  # Debug
