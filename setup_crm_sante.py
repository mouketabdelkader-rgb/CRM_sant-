import sqlite3
import pandas as pd
import requests
import zipfile
import os
from datetime import datetime

# --- CONFIGURATION ---
DB_NAME = "crm_sante.db"
ZIP_URL = "https://service.annuaire.sante.fr/annuaire-sante-webservices/V300/services/extraction/PS_LibreAcces"
ZIP_PATH = "PS_LibreAcces.zip"
CHUNK_SIZE = 100000

def creer_base_de_donnees():
    """Crée la base de données et les 3 tables si elles n'existent pas."""
    print(f"Initialisation de la base de données '{DB_NAME}'...")
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS professionnels (
            id_pp TEXT PRIMARY KEY,
            nom TEXT,
            prenom TEXT,
            date_premiere_apparition DATE,
            date_derniere_apparition DATE,
            est_actif BOOLEAN
        )''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS structures (
            id_structure TEXT PRIMARY KEY,
            type_id TEXT,
            nom_structure TEXT,
            date_creation DATE,
            etat_administratif TEXT,
            nom_dirigeant TEXT,
            annee_naissance_dirigeant INTEGER,
            adresse_normalisee TEXT,
            latitude REAL,
            longitude REAL,
            date_dernier_enrichissement DATETIME
        )''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS activites (
            id_activite INTEGER PRIMARY KEY AUTOINCREMENT,
            id_pp TEXT,
            id_structure TEXT,
            profession TEXT,
            mode_exercice TEXT,
            date_maj DATE,
            FOREIGN KEY (id_pp) REFERENCES professionnels (id_pp),
            FOREIGN KEY (id_structure) REFERENCES structures (id_structure)
        )''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activites_id_pp ON activites (id_pp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_activites_id_structure ON activites (id_structure)')
        
        print("Structure de la base de données vérifiée/créée.")

def telecharger_et_dezipper():
    """Télécharge et dézippe le fichier PS_LibreAcces."""
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

def integrer_donnees(txt_filename, date_maj_str):
    """Intègre les données du fichier texte dans la base de données normalisée."""
    print(f"Intégration des données du {date_maj_str}...")
    
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('UPDATE professionnels SET est_actif = FALSE')
        print("Tous les professionnels marqués comme 'inactifs' avant la mise à jour.")

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        # --- CORRECTION FINALE : Passage de l'encodage à 'utf-8' ---
        reader = pd.read_csv(txt_filename, sep='|', header=0, dtype=str, chunksize=CHUNK_SIZE, encoding='utf-8', on_bad_lines='warn')
        
        total_rows = 0
        required_cols_map = {
            "Identifiant PP": "id_pp",
            "Nom d'exercice": "nom",
            "Prénom d'exercice": "prenom",
            "Identifiant technique de la structure": "id_structure",
            "Libellé profession": "profession",
            "Libellé mode exercice": "mode_exercice"
        }

        for chunk in reader:
            chunk.columns = [col.strip() for col in chunk.columns]
            
            missing_cols = set(required_cols_map.keys()) - set(chunk.columns)
            if missing_cols:
                raise ValueError(f"Colonnes manquantes dans le fichier source : {list(missing_cols)}")

            chunk.dropna(subset=["Identifiant PP"], inplace=True)
            total_rows += len(chunk)

            pros_data = chunk[["Identifiant PP", "Nom d'exercice", "Prénom d'exercice"]].drop_duplicates(subset=['Identifiant PP'])
            structures_data = chunk[['Identifiant technique de la structure']].dropna().drop_duplicates()
            activites_data = chunk[["Identifiant PP", "Identifiant technique de la structure", "Libellé profession", "Libellé mode exercice"]]

            cursor.executemany(f'''
                INSERT INTO professionnels (id_pp, nom, prenom, date_premiere_apparition, date_derniere_apparition, est_actif)
                VALUES (?, ?, ?, '{date_maj_str}', '{date_maj_str}', TRUE)
                ON CONFLICT(id_pp) DO UPDATE SET
                    date_derniere_apparition = '{date_maj_str}',
                    est_actif = TRUE;
            ''', pros_data.to_records(index=False))

            cursor.executemany('INSERT OR IGNORE INTO structures (id_structure) VALUES (?)', structures_data.to_records(index=False))

            activites_data['date_maj'] = date_maj_str
            cursor.executemany('INSERT INTO activites (id_pp, id_structure, profession, mode_exercice, date_maj) VALUES (?, ?, ?, ?, ?)', activites_data.to_records(index=False))
            
            conn.commit()
            print(f"  -> {total_rows} lignes traitées...")

    print("Intégration terminée.")
    os.remove(txt_filename)
    print("Nettoyage du fichier .txt terminé.")

if __name__ == "__main__":
    creer_base_de_donnees()
    nom_fichier_txt = telecharger_et_dezipper()
    date_du_jour = datetime.now().strftime('%Y-%m-%d')
    integrer_donnees(nom_fichier_txt, date_du_jour)
    
    print("\n--- Processus terminé avec succès ---")
    print(f"La base de données '{DB_NAME}' est prête et à jour.")
