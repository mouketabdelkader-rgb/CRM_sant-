import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
PAGE_FINESS = "https://www.data.gouv.fr/fr/datasets/fichier-national-des-etablissements-sanitaires-et-sociaux-finess/"
DOWNLOADED_CSV_PATH = os.path.join(os.getcwd(), "finess_temp.csv")
OUTPUT_ENRICH = "liberaux_dpc_enrich.csv"
# --- FICHIERS DE DEBUG ---
DEBUG_SCREENSHOT = "debug_screenshot.png"
DEBUG_PAGE_SOURCE = "debug_page_source.html"

def debug_finess_download(url):
    """
    Lance un navigateur en mode headless pour prendre une capture d'écran et
    sauvegarder le code source de la page vue par le robot.
    """
    print("Lancement du navigateur en mode DIAGNOSTIC...")
    
    options = Options()
    options.add_argument("--headless")
    # On ajoute des arguments pour simuler un vrai navigateur
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.set_preference("general.useragent.override", "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0")


    driver = None
    try:
        service = Service()
        driver = webdriver.Firefox(service=service, options=options)
        driver.get(url)
        print(f"Navigation vers la page : {url}")

        # On attend quelques secondes pour que les éléments potentiellement bloquants (bannière cookie) apparaissent
        time.sleep(5)

        # --- ÉTAPE DE CAPTURE ---
        print(f"Sauvegarde de la capture d'écran dans : {DEBUG_SCREENSHOT}")
        driver.save_screenshot(DEBUG_SCREENSHOT)
        
        print(f"Sauvegarde du code source HTML dans : {DEBUG_PAGE_SOURCE}")
        with open(DEBUG_PAGE_SOURCE, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        
        print("DIAGNOSTIC TERMINÉ. Le script va maintenant s'arrêter.")
        print("Veuillez analyser les fichiers 'debug_screenshot.png' et 'debug_page_source.html'.")

    except Exception as e:
        print(f"Une erreur est survenue pendant le diagnostic : {e}")
    finally:
        if driver:
            driver.quit()
            print("Navigateur fermé.")

if __name__ == "__main__":
    # Nous n'exécutons que la fonction de diagnostic. Le reste est ignoré.
    debug_finess_download(PAGE_FINESS)
