import pandas as pd
import requests
import time
import os
from tqdm import tqdm

# --- CONFIGURATION ---
# Le fichier CSV contenant les professionnels à enrichir
# Nous utiliserons un petit fichier de test pour commencer.
INPUT_CSV = "pros_a_enrichir.csv" 
# Le nom du fichier de sortie qui contiendra les données enrichies
OUTPUT_CSV = "pros_enrichis_sirene.csv"
# Nom de la colonne contenant le SIRET/FINESS
SIRET_COLUMN = "Identifiant technique de la structure"

def is_siret(identifier):
    """Vérifie si un identifiant est un SIRET valide (14 chiffres)."""
    # Assure que l'identifiant est une chaîne de caractères avant les vérifications
    identifier = str(identifier).strip()
    return identifier.isdigit() and len(identifier) == 14

def enrichir_siret(siret):
    """
    Interroge l'API Recherche d'entreprises et retourne un dictionnaire
    avec les informations clés.
    """
    api_url = f"https://recherche-entreprises.api.gouv.fr/search?q={siret}"
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('results'):
            entreprise_info = data['results'][0]
            
            dirigeant_nom, dirigeant_naissance = None, None
            if entreprise_info.get('dirigeants'):
                dirigeant = entreprise_info['dirigeants'][0]
                dirigeant_nom = f"{dirigeant.get('prenoms', '')} {dirigeant.get('nom', '')}".strip()
                dirigeant_naissance = dirigeant.get('annee_de_naissance')

            return {
                "siret_date_creation": entreprise_info.get('date_creation'),
                "siret_etat_administratif": entreprise_info.get('etat_administratif'),
                "siret_dirigeant_principal": dirigeant_nom,
                "siret_dirigeant_naissance": dirigeant_naissance,
                "enrichissement_statut": "Succès"
            }
        else:
            return {"enrichissement_statut": "Non trouvé"}
            
    except requests.exceptions.RequestException:
        return {"enrichissement_statut": "Erreur API"}
    except (KeyError, IndexError, json.JSONDecodeError):
        return {"enrichissement_statut": "Données invalides"}

if __name__ == "__main__":
    if not os.path.exists(INPUT_CSV):
        print(f"ERREUR: Le fichier d'entrée '{INPUT_CSV}' est introuvable.")
        exit(1)
        
    print(f"Lecture du fichier : {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)
    
    results = []
    
    print(f"Enrichissement de {len(df)} professionnels via l'API SIRENE...")
    
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Progression"):
        siret = row.get(SIRET_COLUMN)
        
        enriched_data = {}
        if pd.notna(siret) and is_siret(siret):
            enriched_data = enrichir_siret(siret)
            # Fait une pause de 50ms pour respecter l'API et éviter les blocages
            time.sleep(0.05) 
        else:
            enriched_data["enrichissement_statut"] = "Non-SIRET"
        
        # Combine les données originales et les données enrichies
        new_row = {**row.to_dict(), **enriched_data}
        results.append(new_row)

    df_enriched = pd.DataFrame(results)

    df_enriched.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    
    print("\n--- Enrichissement Terminé ---")
    print(f"Résultats sauvegardés dans : {OUTPUT_CSV}")
    
    statut_counts = df_enriched["enrichissement_statut"].value_counts()
    print("\nRésumé de l'opération :")
    print(statut_counts)
