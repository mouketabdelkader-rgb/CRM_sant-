import sqlite3
import pandas as pd
import os

# --- CONFIGURATION ---
DB_NAME = "crm_sante.db"
OUTPUT_CSV = "pros_a_enrichir.csv"
LIMIT_ROWS = 5000  # On va chercher 5000 lignes avec un SIRET valide

def create_test_file_with_siret():
    """
    Extrait un échantillon de la base de données 'crm_sante.db'
    en ne sélectionnant QUE les entrées avec un SIRET valide.
    """
    if not os.path.exists(DB_NAME):
        print(f"ERREUR: La base de données '{DB_NAME}' est introuvable.")
        return

    print(f"Extraction d'un échantillon de {LIMIT_ROWS} lignes avec SIRET depuis '{DB_NAME}'...")
    
    with sqlite3.connect(DB_NAME) as conn:
        # --- REQUÊTE AMÉLIORÉE ---
        # On ne sélectionne que les lignes où l'id_structure a 14 caractères et est composé uniquement de chiffres.
        query = f"""
            SELECT
                p.nom AS "Nom d'exercice",
                p.prenom AS "Prénom d'exercice",
                a.id_structure AS "Identifiant technique de la structure"
            FROM professionnels p
            JOIN activites a ON p.id_pp = a.id_pp
            WHERE p.est_actif = TRUE
              AND LENGTH(a.id_structure) = 14 
              AND a.id_structure GLOB '[0-9]*'
            LIMIT {LIMIT_ROWS}
        """
        df_sample = pd.read_sql_query(query, conn)

    if df_sample.empty:
        print("Aucun SIRET valide n'a pu être extrait. La base de données ne contient peut-être pas de libéraux.")
        return
        
    df_sample.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Fichier de test '{OUTPUT_CSV}' créé avec succès, contenant {len(df_sample)} lignes avec SIRET.")

if __name__ == "__main__":
    create_test_file_with_siret()
