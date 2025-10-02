import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "crm_sante.db"
OUTPUT_CSV = f"nouveaux_pros_{datetime.now().strftime('%Y-%m-%d')}.csv"
DATE_MAJ = datetime.now().strftime('%Y-%m-%d')

def detecter_nouveaux():
    """Interroge la table 'professionnels' pour trouver les nouveaux du jour."""
    print(f"Détection des nouveaux professionnels pour la date : {DATE_MAJ}")
    with sqlite3.connect(DB_NAME) as conn:
        # La requête devient beaucoup plus simple !
        query = f"SELECT * FROM professionnels WHERE date_premiere_apparition = '{DATE_MAJ}'"
        df_nouveaux = pd.read_sql_query(query, conn)

    if df_nouveaux.empty:
        print("Aucun nouveau professionnel détecté aujourd'hui.")
        return 0

    # Pour obtenir plus de détails, on fait une jointure
    with sqlite3.connect(DB_NAME) as conn:
        query_details = f"""
            SELECT p.id_pp, p.nom, p.prenom, a.profession, a.mode_exercice, a.id_structure
            FROM professionnels p
            JOIN activites a ON p.id_pp = a.id_pp
            WHERE p.date_premiere_apparition = '{DATE_MAJ}'
            AND a.date_maj = '{DATE_MAJ}'
        """
        df_details = pd.read_sql_query(query_details, conn)

    df_details.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"Exporté {len(df_details)} activités de nouveaux professionnels dans {OUTPUT_CSV}")
    return len(df_details)

if __name__ == "__main__":
    detecter_nouveaux()
