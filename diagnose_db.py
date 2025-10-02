import sqlite3
import pandas as pd
import os

DB_NAME = "crm_sante.db"

def diagnose_ids():
    """
    Extrait un échantillon d'id_structure de la base pour analyser leur format.
    """
    if not os.path.exists(DB_NAME):
        print(f"ERREUR: La base de données '{DB_NAME}' est introuvable.")
        return

    print(f"--- DIAGNOSTIC DES IDENTIFIANTS DE STRUCTURE DANS '{DB_NAME}' ---")
    
    with sqlite3.connect(DB_NAME) as conn:
        # On extrait 100 identifiants au hasard pour analyse
        query = "SELECT id_structure FROM structures ORDER BY RANDOM() LIMIT 100"
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("La table 'structures' est vide. Aucune analyse possible.")
        return
        
    print("Voici un échantillon de 100 'id_structure' et leur format :\n")
    for index, row in df.iterrows():
        id_struct = row['id_structure']
        if id_struct:
            # Affiche l'identifiant, son type Python, et sa longueur
            print(f"ID: '{id_struct}' | Type: {type(id_struct)} | Longueur: {len(str(id_struct))}")
        else:
            print(f"ID: '{id_struct}' (Valeur nulle ou vide)")
            
    print("\n--- FIN DU DIAGNOSTIC ---")
    print("Veuillez analyser la sortie ci-dessus. Les SIRET devraient être des chaînes de 14 caractères.")


if __name__ == "__main__":
    diagnose_ids()
