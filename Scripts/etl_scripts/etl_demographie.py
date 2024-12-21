# Importation des librairies
import os
import pandas as pd
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

# Mise en place de l'environnement
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
input_path = "output/demographie_datas_clean"

"""
    Cette fonction réalise le processus ETL pour les données démographiques.

    1. Charge les données depuis un fichier CSV contenant des informations démographiques.
    2. Supprime la colonne 'Code' qui n'est pas nécessaire pour l'analyse.
    3. Renomme les colonnes pour qu'elles aient des noms plus explicites et adaptés à la base de données.
    4. Crée une table de staging dans PostgreSQL et insère les données.

    Le processus inclut l'insertion des données dans la table 'stg_demographie' de la base de données PostgreSQL.

    Utilisation de la bibliothèque 'pandas' pour la manipulation des données et 'SQLAlchemy' pour l'insertion dans PostgreSQL.
    """


def process_etl_demographie():
    df_demographie = pd.read_csv(
        os.path.join(input_path, "/transf_demographie.csv"), sep=","
    )

    # Préparation de la table de staging
    df_demographie = df_demographie.drop(columns="Code")

    stg_demographie = df_demographie.rename(
        columns={
            "Libellé": "nom_commune",
            "Unités légales (en nombre) 2021": "unite_legal_2021",
            "Population municipale 2022": "pop22",
            "Densité de population (historique depuis 1876) 2021": "dens1876_2021",
            "Part construction dans les créations d'ent. 2023": "part_construction_entreprise_2023",
        }
    )

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    stg_demographie.to_sql("stg_demographie", engine, if_exists="append", index=False)


if __name__ == "__main__":
    process_etl_demographie()
