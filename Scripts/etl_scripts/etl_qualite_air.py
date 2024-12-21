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
    Cette fonction réalise le processus ETL pour les données de qualité de l'air.

    1. Charge les données depuis un fichier CSV contenant les mesures de qualité de l'air.
    2. Crée une table de dimension pour les polluants ('dim_pollution') et une table de dimension pour les dates ('dim_date').
    3. Renomme les colonnes dans les données pour les adapter aux tables de staging.
    4. Insère les données transformées dans PostgreSQL dans les tables 'dim_pollution', 'dim_date', et 'stg_air'.

    Utilisation de la bibliothèque 'pandas' pour la transformation des données et 'SQLAlchemy' pour l'insertion dans PostgreSQL.
    """


def process_etl_air():
    df_air_final = pd.read_csv(
        os.path.join(input_path, "/transf_qualite_air.csv"), sep=","
    )

    # Création des dataframes qui vont charger les tables postgreSQL
    dim_polluant = df_air_final[["Polluant"]].drop_duplicates()
    dim_polluant = dim_polluant.rename(columns={"Polluant": "nom_polluant"})

    dim_date = df_air_final[["Date de début"]].drop_duplicates()
    dim_date["date"] = pd.to_datetime(dim_date["Date de début"])
    dim_date["id_date"] = dim_date["date"].dt.strftime("%Y%m%d")
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["day"] = dim_date["date"].dt.day
    dim_date = dim_date.drop(columns="Date de début")

    df_air_final = df_air_final[
        ["Date de début", "code site", "Polluant", "valeur brute", "unité de mesure"]
    ]
    stg_air = df_air_final.rename(
        columns={
            "Polluant": "nom_polluant",
            "Date de début": "date",
            "code site": "code_site",
            "valeur brute": "valeur_brute",
            "unité de mesure": "code_unite_mesure",
        }
    )
    stg_air["date"] = pd.to_datetime(stg_air["date"])

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    dim_polluant.to_sql("dim_pollution", engine, if_exists="append", index=False)
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
    stg_air.to_sql("stg_air", engine, if_exists="append", index=False)


if __name__ == "__main__":
    process_etl_air()
