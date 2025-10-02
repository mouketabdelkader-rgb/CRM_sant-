import sqlite3
import pandas as pd
import os

# --- CONFIGURATION ---
DB_NAME = "crm_sante.db"

def run_tests():
    """Exécute une série de tests sur la base de données crm_sante.db."""
    if not os.path.exists(DB_NAME):
        print(f"--- ❌ ÉCHEC ---")
        print(f"Le fichier de base de données '{DB_NAME}' est introuvable.")
        return

    print(f"--- ✅ RAPPORT DE VÉRIFICATION POUR '{DB_NAME}' ---")
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()

            # Test 1: Vérification de l'existence des tables
            print("\n1. Vérification des tables...")
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)['name'].tolist()
            required_tables = ['professionnels', 'structures', 'activites']
            test_ok = True
            for table in required_tables:
                if table in tables:
                    print(f"  - Table '{table}' : OK")
                else:
                    print(f"  - Table '{table}' : ❌ MANQUANTE")
                    test_ok = False
            if not test_ok: return

            # Test 2: Comptage des lignes
            print("\n2. Comptage des lignes...")
            count_pros = pd.read_sql("SELECT COUNT(*) FROM professionnels", conn).iloc[0,0]
            count_structs = pd.read_sql("SELECT COUNT(*) FROM structures", conn).iloc[0,0]
            count_acts = pd.read_sql("SELECT COUNT(*) FROM activites", conn).iloc[0,0]
            print(f"  - Professionnels uniques : {count_pros:,}")
            print(f"  - Structures uniques     : {count_structs:,}")
            print(f"  - Activités enregistrées : {count_acts:,}")
            if not (count_pros > 1000000 and count_structs > 500000 and count_acts > 1000000):
                 print("  - ⚠️ Avertissement : Le nombre de lignes semble bas.")

            # Test 3: Vérification du statut "est_actif"
            print("\n3. Vérification du statut d'activité...")
            inactifs = pd.read_sql("SELECT COUNT(*) FROM professionnels WHERE est_actif = FALSE", conn).iloc[0,0]
            if inactifs == 0:
                print(f"  - Nombre de professionnels inactifs : {inactifs}. OK (premier import)")
            else:
                print(f"  - ⚠️ Avertissement : {inactifs} professionnels sont marqués comme inactifs.")

            # Test 4: "Spot check" sur un professionnel au hasard
            print("\n4. Test de cohérence sur un échantillon...")
            df_sample = pd.read_sql('''
                SELECT
                    p.id_pp,
                    p.nom,
                    p.prenom,
                    a.profession,
                    a.mode_exercice,
                    s.id_structure
                FROM professionnels p
                JOIN activites a ON p.id_pp = a.id_pp
                JOIN structures s ON a.id_structure = s.id_structure
                WHERE p.est_actif = TRUE
                LIMIT 5
            ''', conn)
            
            if not df_sample.empty:
                print("  - Échantillon de données cohérent. Voici les 5 premières lignes :")
                print(df_sample.to_string())
            else:
                print("  - ❌ ÉCHEC : Impossible de récupérer un échantillon cohérent.")

    except Exception as e:
        print(f"\n--- ❌ UNE ERREUR EST SURVENUE PENDANT LES TESTS ---")
        print(e)
        return
        
    print("\n--- ✅ FIN DU RAPPORT ---")

if __name__ == "__main__":
    run_tests()
