# Importation des librairies
import os
import pandas as pd

# chemin d'acces aux fichiers sources
input_path = "Datalake/input/air_measurement_datas"


def process_qualite_air():

    # chargement et selection des données de qualite de l'air dans des dataframes
    df_co = pd.read_csv(
        os.path.join(input_path, "/CO/CO - 2022-01-01 00_00 - 2024-12-20.csv"),
        sep=";",
        usecols=[
            "Date de début",
            "Date de fin",
            "Organisme",
            "code site",
            "type d'implantation",
            "Polluant",
            "valeur brute",
            "unité de mesure",
            "validité",
        ],
    )
    df_no2 = pd.read_csv(
        os.path.join(input_path, "/NO2/NO₂ - 2022-01-01 00_00 - 2024-12-20.csv"),
        sep=";",
        usecols=[
            "Date de début",
            "Date de fin",
            "Organisme",
            "code site",
            "type d'implantation",
            "Polluant",
            "valeur brute",
            "unité de mesure",
            "validité",
        ],
    )
    df_o3 = pd.read_csv(
        os.path.join(input_path, "/O3/O₃ - 2022-01-01 00_00 - 2024-12-20.csv"),
        sep=";",
        usecols=[
            "Date de début",
            "Date de fin",
            "Organisme",
            "code site",
            "type d'implantation",
            "Polluant",
            "valeur brute",
            "unité de mesure",
            "validité",
        ],
    )
    df_pm10 = pd.read_csv(
        os.path.join(input_path, "/PM10/PM₁₀ - 2022-01-01 00_00 - 2024-12-20.csv"),
        sep=";",
        usecols=[
            "Date de début",
            "Date de fin",
            "Organisme",
            "code site",
            "type d'implantation",
            "Polluant",
            "valeur brute",
            "unité de mesure",
            "validité",
        ],
    )
    df_pm25 = pd.read_csv(
        os.path.join(input_path, "/PM25/PM₂.₅ - 2022-01-01 00_00 - 2024-12-20.csv"),
        sep=";",
        usecols=[
            "Date de début",
            "Date de fin",
            "Organisme",
            "code site",
            "type d'implantation",
            "Polluant",
            "valeur brute",
            "unité de mesure",
            "validité",
        ],
    )

    # concaténation des différents dataframe en un seul : df_all
    df_all = pd.concat([df_co, df_no2, df_o3, df_pm10, df_pm25], ignore_index=True)

    # filtre sur les donnees valides/mesurables (validité = 1)
    df_air = df_all[df_all["validité"] == 1]
    df_air_final = df_air.drop(columns=["validité"])

    # chemin d'acces au fichier exporté
    output_path = "output/air_measurement_datas_clean/"

    # Export du dataframe nettoyé
    df_air_final.to_csv(
        output_path + "transf_qualite_air.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    process_qualite_air()
