import pandas as pd
import os

# --- CONFIGURATION ---
# Le nom du fichier peut varier, on le trouve dynamiquement
TXT_FILENAME = next((f for f in os.listdir('.') if 'Personne_activite' in f and f.endswith('.txt')), None)
CHUNK_SIZE = 500000 # On analyse par lots de 500 000 lignes

def analyze_full_file():
    """
    Analyse l'intégralité du fichier PS_LibreAcces par chunks pour compter les SIRET.
    """
    if not TXT_FILENAME:
        print("ERREUR: Fichier 'Personne_activite...txt' introuvable.")
        print("Veuillez d'abord lancer 'setup_crm_sante.py' en mode diagnostic pour le télécharger.")
        return

    print(f"--- ANALYSE COMPLÈTE DU FICHIER '{TXT_FILENAME}' ---")
    
    total_rows = 0
    total_sirets = 0
    first_siret_found_in_chunk = -1
    chunk_number = 0

    try:
        reader = pd.read_csv(
            TXT_FILENAME,
            sep='|',
            header=0,
            dtype=str,
            encoding='utf-8',
            on_bad_lines='warn',
            chunksize=CHUNK_SIZE
        )
        
        id_structure_col = "Identifiant technique de la structure"

        for chunk in reader:
            chunk_number += 1
            chunk.columns = [col.strip() for col in chunk.columns]
            
            if id_structure_col not in chunk.columns:
                raise ValueError(f"La colonne '{id_structure_col}' est introuvable.")

            total_rows += len(chunk)
            
            siret_series = chunk[id_structure_col].dropna()
            sirets_in_chunk = siret_series[siret_series.str.match(r'^\d{14}$')].count()
            
            total_sirets += sirets_in_chunk
            
            if sirets_in_chunk > 0 and first_siret_found_in_chunk == -1:
                first_siret_found_in_chunk = chunk_number
            
            print(f"  -> Chunk #{chunk_number}: {len(chunk)} lignes traitées, {sirets_in_chunk} SIRET trouvés.")

    except Exception as e:
        print(f"Une erreur est survenue pendant l'analyse : {e}")
        return

    print("\n--- RAPPORT FINAL ---")
    print(f"Lignes totales analysées : {total_rows:,}")
    print(f"SIRET valides trouvés    : {total_sirets:,}")

    if total_sirets > 0:
        print(f"✅ SUCCÈS : Des SIRET ont bien été trouvés dans le fichier.")
        print(f"Les premiers SIRET sont apparus dans le chunk #{first_siret_found_in_chunk}.")
        print("Cela confirme que le fichier est trié et que notre logique d'import est correcte.")
    else:
        print("❌ ÉCHEC : Aucun SIRET valide n'a été trouvé dans l'intégralité du fichier.")

if __name__ == "__main__":
    analyze_full_file()
