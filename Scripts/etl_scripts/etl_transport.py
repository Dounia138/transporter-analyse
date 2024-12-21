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
input_path = "output/transport_datas_clean"

"""
    Cette fonction réalise le processus ETL pour les données de transport.

    1. Charge les données depuis un fichier CSV contenant des informations sur les transports.
    2. Crée une table de dimension pour les types de transport ('dim_transport').
    3. Renomme les colonnes pour adapter les données aux tables de staging.
    4. Insère les données dans les tables 'dim_transport' et 'stg_transport' dans PostgreSQL.

    Utilisation de la bibliothèque 'pandas' pour la transformation des données et 'SQLAlchemy' pour l'insertion dans PostgreSQL.
"""


def process_etl_transport():
    df_transport = pd.read_csv(
        os.path.join(input_path, "/transf_transport.csv"), sep=","
    )

    # création des dataframes qui vont remplir les tables postgreSQL
    dim_transport = df_transport[["ArRType"]].drop_duplicates()
    dim_transport = dim_transport.rename(columns={"ArRType": "nom_transport"})

    stg_transport = df_transport.rename(
        columns={
            "ArRType": "nom_transport",
            "ArRTown": "nom_commune",
            "NombreDeTransport": "nb_transport",
            "Annee": "year",
        }
    )

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    dim_transport.to_sql("dim_transport", engine, if_exists="append", index=False)
    stg_transport.to_sql("stg_transport", engine, if_exists="append", index=False)


if __name__ == "__main__":
    process_etl_transport()
