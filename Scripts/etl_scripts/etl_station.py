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
input_path = "output/air_measurement_datas_clean"

"""
    Cette fonction réalise le processus ETL pour les données des stations de mesure.

    1. Charge les données depuis un fichier CSV contenant des informations sur les stations.
    2. Crée une table de dimension pour les communes ('dim_commune') et sélectionne uniquement les données pour la commune 'NEUILLY-SUR-SEINE'.
    3. Transforme les données des stations, renomme les colonnes, et filtre les stations pour la commune 'NEUILLY-SUR-SEINE'.
    4. Insère les données dans les tables 'dim_commune' et 'stg_station' dans PostgreSQL.

    Utilisation de la bibliothèque 'pandas' pour la transformation des données et 'SQLAlchemy' pour l'insertion dans PostgreSQL.
"""


def process_etl_station():
    df_station = pd.read_csv(os.path.join(input_path, "/transf_station.csv"), sep=",")

    df_commune = df_station[["Commune", "Longitude", "Latitude"]]
    df_commune = df_commune.rename(
        columns={"Commune": "nom_commune", "Longitude": "lont", "Latitude": "lat"}
    )
    dim_commune = df_commune[df_commune["nom_commune"] == "NEUILLY-SUR-SEINE"]

    df_station = df_station[["Code", "Nom", "Commune", "Implantation"]]
    df_station = df_station.rename(
        columns={
            "Code": "id_station",
            "Nom": "nom_station",
            "Commune": "nom_commune",
            "Implantation": "type_implant",
        }
    )
    stg_station = df_station[df_station["nom_commune"] == "NEUILLY-SUR-SEINE"]

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    dim_commune.to_sql("dim_commune", engine, if_exists="append", index=False)
    stg_station.to_sql("stg_station", engine, if_exists="append", index=False)


if __name__ == "__main__":
    process_etl_station()
