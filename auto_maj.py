import requests
from datetime import datetime, timezone
import os

ZIP_URL = "https://service.annuaire.sante.fr/annuaire-sante-webservices/V300/services/extraction/PS_LibreAcces"
LAST_MODIFIED_FILE = "last_modified_timestamp.txt"

def get_remote_last_modified(url):
    """Effectue une requête HEAD pour obtenir la date de dernière modification du fichier distant."""
    try:
        response = requests.head(url, verify=False, timeout=20)
        response.raise_for_status()
        last_modified_str = response.headers.get('Last-Modified')
        if not last_modified_str: return None
        remote_time = datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')
        return remote_time.replace(tzinfo=timezone.utc)
    except requests.exceptions.RequestException:
        return None

def get_local_last_modified(file_path):
    """Lit la date de dernière modification depuis le fichier local."""
    if not os.path.exists(file_path): return None
    with open(file_path, 'r') as f:
        return datetime.fromisoformat(f.read().strip())

if __name__ == "__main__":
    remote_time = get_remote_last_modified(ZIP_URL)
    local_time = get_local_last_modified(LAST_MODIFIED_FILE)
    
    if remote_time and (not local_time or remote_time > local_time):
        print("is_new_update=true") # Sortie standard pour le workflow
        # Met à jour le timestamp pour les prochains runs
        with open(LAST_MODIFIED_FILE, 'w') as f:
            f.write(remote_time.isoformat())
    else:
        print("is_new_update=false")
