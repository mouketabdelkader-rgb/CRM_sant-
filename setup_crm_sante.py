import pandas as pd
import requests
import zipfile
import os

# --- CONFIGURATION ---
ZIP_URL = "https://service.annuaire.sante.fr/annuaire-sante-webservices/V300/services/extraction/PS_LibreAcces"
ZIP_PATH = "PS_LibreAcces.zip"

def telecharger_et_dezipper():
    """Télécharge et dézippe le fichier PS_LibreAcces."""
    if os.path.exists("PS_LibreAcces_Personne_activite_202510020831.txt"):
        print("Fichier .txt déjà présent, on ne retélécharge pas.")
        return "PS_LibreAcces_Personne_activite_202510020831.txt"
        
    print(f"Téléchargement de {ZIP_URL}...")
    with requests.get(ZIP_URL, stream=True, verify=False) as r:
        r.raise_for_status()
        with open(ZIP_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print(f"Décompression de {ZIP_PATH}...")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        txt_filename = next((name for name in zip_ref.namelist() if 'Personne_activite' in name), None)
        if not txt_filename:
            raise FileNotFoundError("Le fichier 'Personne_activite' est introuvable dans le ZIP.")
        zip_ref.extract(txt_filename, path=".")
    
    os.remove(ZIP_PATH)
    return txt_filename

def diagnose_source_file(txt_filename):
    """
    Lit le premier chunk du fichier source et affiche un diagnostic
    sur la colonne 'Identifiant technique de la structure'.
    """
    print("\n--- DIAGNOSTIC DU FICHIER SOURCE ---")
    
    try:
        # Lire seulement le premier chunk pour le diagnostic
        reader = pd.read_csv(
            txt_filename,
            sep='|',
            header=0,
            dtype=str,  # Force la lecture de toutes les colonnes comme du texte
            encoding='utf-8',
            on_bad_lines='warn',
            nrows=200000 # On lit un gros premier chunk pour être sûr d'avoir des SIRET
        )
        
        # Nettoyer les noms de colonnes
        reader.columns = [col.strip() for col in reader.columns]
        
        id_structure_col = "Identifiant technique de la structure"
        
        if id_structure_col not in reader.columns:
            print(f"ERREUR: La colonne '{id_structure_col}' est introuvable dans le fichier source.")
            print(f"Colonnes trouvées : {reader.columns.tolist()}")
            return

        print(f"Analyse de la colonne '{id_structure_col}' sur les 200 000 premières lignes...\n")
        
        # On filtre pour ne garder que les SIRET potentiels (14 caractères numériques)
        siret_series = reader[id_structure_col].dropna()
        siret_series = siret_series[siret_series.str.match(r'^\d{14}$')]

        if siret_series.empty:
            print("--- RÉSULTAT ---")
            print("❌ Aucun SIRET valide (14 chiffres) n'a été trouvé dans l'échantillon.")
            print("Cela confirme un problème dans la lecture ou le format du fichier source.")
            print("\nAffichage d'un échantillon brut de la colonne pour analyse :")
            print(reader[id_structure_col].dropna().head(20))
        else:
            print("--- RÉSULTAT ---")
            print("✅ Des SIRET valides ont bien été trouvés dans le fichier source !")
            print("Le problème vient donc de la logique d'insertion dans la base de données.")
            print("\nVoici les 10 premiers SIRET trouvés :")
            print(siret_series.head(10).to_string())

    except Exception as e:
        print(f"Une erreur est survenue pendant le diagnostic : {e}")

if __name__ == "__main__":
    nom_fichier_txt = telecharger_et_dezipper()
    diagnose_source_file(nom_fichier_txt)
