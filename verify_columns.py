import pandas as pd
import os

# --- CONFIGURATION ---
# Trouve dynamiquement le nom du fichier source
TXT_FILENAME = next((f for f in os.listdir('.') if 'Personne_activite' in f and f.endswith('.txt')), None)

def verify_columns_content():
    """
    Affiche un échantillon comparatif des colonnes d'identification pour analyse.
    """
    if not TXT_FILENAME:
        print("ERREUR: Fichier 'Personne_activite...txt' introuvable.")
        return

    print(f"--- ANALYSE COMPARATIVE DES COLONNES DANS '{TXT_FILENAME}' ---")
    
    try:
        df = pd.read_csv(
            TXT_FILENAME,
            sep='|',
            header=0,
            dtype=str,
            encoding='utf-8',
            on_bad_lines='warn',
            nrows=100000
        )
        
        df.columns = [col.strip() for col in df.columns]

        # Les colonnes que nous voulons comparer
        cols_to_check = [
            "Libellé mode exercice",
            "Identifiant technique de la structure",
            "Numéro SIRET site",
            "Numéro SIREN site",
            "Numéro FINESS site"
        ]
        
        # On s'assure qu'elles existent
        for col in cols_to_check:
            if col not in df.columns:
                print(f"Avertissement : Colonne '{col}' non trouvée dans l'échantillon.")
                return

        # On filtre pour ne garder que les lignes qui ont au moins un de ces identifiants
        df_filtered = df[cols_to_check].dropna(subset=["Identifiant technique de la structure", "Numéro SIRET site", "Numéro FINESS site"], how='all')

        print("\nVoici un échantillon des identifiants trouvés. Cherchons visuellement les SIRET :\n")
        
        # On affiche les 50 premières lignes pertinentes
        print(df_filtered.head(50).to_string())

        print("\n--- FIN DE L'ANALYSE ---")

    except Exception as e:
        print(f"Une erreur est survenue pendant l'analyse : {e}")

if __name__ == "__main__":
    verify_columns_content()
