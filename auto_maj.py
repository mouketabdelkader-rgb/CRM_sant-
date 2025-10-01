import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import subprocess
import re

# Config
PAGE_URL = "https://annuaire.sante.fr/en/web/site-pro/extractions-publiques"
LAST_MAJ_FILE = "last_maj_timestamp.txt"
ZIP_URL = "https://service.annuaire.sante.fr/annuaire-sante-webservices/V300/services/extraction/PS_LibreAcces"

def get_latest_date_from_page():
    print("Scraping de la page Annuaire Santé...")
    response = requests.get(PAGE_URL, verify=False)  # Bypass SSL comme avant
    if response.status_code != 200:
        raise Exception(f"Erreur scraping: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'lxml')
    # Cherche la table avec dates (basé sur structure page : td avec date/heure)
    table = soup.find('table')  # Table principale des extractions
    if not table:
        raise Exception("Table des extractions non trouvée")
    
    rows = table.find_all('tr')
    latest_date = None
    latest_timestamp = None
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 3:
            date_cell = cells[0].text.strip()
            heure_cell = cells[1].text.strip()
            titre_cell = cells[2].text.strip()
            if 'PS_LibreAcces' in titre_cell or 'Extraction_des_PS_autorises_a_exercer' in titre_cell:
                # Parse date/heure (ex. : 29/09/2025 08:27)
                date_match = re.match(r'(\d{2}/\d{2}/\d{4})', date_cell)
                if date_match:
                    full_date = f"{date_match.group(1)} {heure_cell}"  # Ajoute heure
                    dt = datetime.strptime(full_date, '%d/%m/%Y %H:%M')
                    if latest_date is None or dt > latest_date:
                        latest_date = dt
                        latest_timestamp = full_date
    
    if latest_date:
        print(f"Dernière MAJ détectée: {latest_timestamp}")
        return latest_timestamp
    raise Exception("Aucune date MAJ trouvée pour PS_LibreAcces")

def is_new_update(current_timestamp):
    if not os.path.exists(LAST_MAJ_FILE):
        return True
    with open(LAST_MAJ_FILE, 'r') as f:
        last_timestamp = f.read().strip()
    
    try:
        # Essaie format principal (page)
        last_dt = datetime.strptime(last_timestamp, '%d/%m/%Y %H:%M')
    except ValueError:
        try:
            # Essaie format ancien (filename : YYYYMMDDHHMM)
            last_dt = datetime.strptime(last_timestamp, '%Y%m%d%H%M')
        except ValueError:
            print(f"Format timestamp inconnu: {last_timestamp} – Force MAJ")
            return True
    
    curr_dt = datetime.strptime(current_timestamp, '%d/%m/%Y %H:%M')
    return curr_dt > last_dt

def update_timestamp(timestamp):
    with open(LAST_MAJ_FILE, 'w') as f:
        f.write(timestamp)

if __name__ == "__main__":
    try:
        current_timestamp = get_latest_date_from_page()
        if is_new_update(current_timestamp):
            print("Nouvelle MAJ détectée – Lancement download et détection...")
            # Télécharge et setup (capture output pour logs clean)
            result_setup = subprocess.run(['python', 'setup_crm_sante.py'], capture_output=True, text=True, check=True)
            print("Setup terminé:", result_setup.stdout)
            if result_setup.stderr:
                print("Warning setup:", result_setup.stderr)
            # Détection nouveaux (AVANT rename)
            print("Lancement détection nouveaux (previous vs current)...")
            result_detect = subprocess.run(['python', 'detect_nouveaux.py'], capture_output=True, text=True, check=True)
            print("Détection terminée:", result_detect.stdout)
            if result_detect.stderr:
                print("Warning detect:", result_detect.stderr)
            # Renomme DBs APRÈS détection
            if os.path.exists('crm_sante_current.db'):
                if os.path.exists('crm_sante_previous.db'):
                    print("Warning: previous existe déjà – Backup en previous_old.db")
                    os.rename('crm_sante_previous.db', 'crm_sante_previous_old.db')
                os.rename('crm_sante_current.db', 'crm_sante_previous.db')
                print("Current renommé en previous – Prêt pour next run.")
            # Met à jour timestamp
            update_timestamp(current_timestamp)
            print("MAJ terminée !")
        else:
            print("Aucune nouvelle MAJ – Skip.")
    except Exception as e:
        print(f"Erreur auto_maj: {e}")
