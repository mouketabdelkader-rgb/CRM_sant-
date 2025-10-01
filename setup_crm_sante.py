import requests
import zipfile
import pandas as pd
import sqlite3
import os
import glob
from datetime import datetime
import time  # Pour flush disque

# Config
URL = "https://service.annuaire.sante.fr/annuaire-sante-webservices/V300/services/extraction/PS_LibreAcces"
DB_FILE = "crm_sante_current.db"
DELIMITER = '|'

def download_and_extract(url, zip_path="temp.zip"):
    print("Téléchargement en cours... (Bypass SSL activé pour test ? non recommandé en prod)")
    response = requests.get(url, stream=True, verify=False)
    if response.status_code == 200:
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        file_size = os.path.getsize(zip_path)
        print(f"Téléchargé: {file_size} bytes (~{file_size / (1024*1024):.1f} Mo)")
        if file_size < 100 * 1024 * 1024:
            print("Warning: Fichier trop petit ? vérifie l'URL ou réseau.")
    else:
        raise Exception(f"Erreur téléchargement: {response.status_code} - {response.text[:200]}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall("data/")
    os.remove(zip_path)
    print("Extraction terminée dans ./data/")

def parse_and_load_to_db():
    data_dir = "data/"
    pattern = os.path.join(data_dir, "PS_LibreAcces_Personne_activite_*.txt")
    txt_files = glob.glob(pattern)
    if not txt_files:
        raise Exception(f"Aucun fichier trouvé pour le pattern: {pattern}")
    txt_file = txt_files[0]
    print(f"Fichier détecté: {os.path.basename(txt_file)}")
    
    full_path = txt_file
    
    print(f"Parsing de {os.path.basename(txt_file)}...")
    df = pd.read_csv(full_path, sep=DELIMITER, encoding='utf-8', low_memory=False, encoding_errors='replace')
    print(f"Lignes lues: {len(df)}")
    print("Colonnes détectées (10 premières):", df.columns.tolist()[:10])
    
    # Nettoyage : drop sans 'Identifiant PP'
    if 'Identifiant PP' in df.columns:
        df_clean = df.dropna(subset=['Identifiant PP'])
        print(f"Lignes après nettoyage: {len(df_clean)}")
    else:
        print("Warning: 'Identifiant PP' non trouvé ? sans drop.")
        df_clean = df
    
    # Ajoute date_maj
    df_clean = df_clean.copy()
    df_clean['date_maj'] = datetime.now().strftime('%Y-%m-%d')
    
    # Insertion (to_sql crée la table auto avec toutes les colonnes)
    conn = sqlite3.connect(DB_FILE)
    df_clean.to_sql('personnes_activites', conn, if_exists='replace', index=False)
    conn.commit()
    time.sleep(2)  # Force flush disque pour subprocess detect_nouveaux.py
    print("DB flushée avec sleep(2) - Table prête pour détection.")
    conn.close()
    print(f"Inséré {len(df_clean)} lignes (toutes colonnes). DB: {DB_FILE}")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    # download_and_extract(URL)  # Commenté pour test – Décommente pour full run
    parse_and_load_to_db()
    print("Setup terminé ! Vérifie avec: sqlite3 crm_sante_current.db 'SELECT COUNT(*) FROM personnes_activites;'")
