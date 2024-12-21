# Importation des librairies
import os
import pandas as pd

# chemin d'acces aux fichiers sources
input_path = "Datalake/input/air_measurement_datas"


def process_station():

    # chargement et selection des données de balises dans le dataframe
    df_station = pd.read_csv(
        os.path.join(input_path, "/Station/stations.csv"),
        sep=";",
        usecols=["Commune", "Longitude", "Latitude", "Code", "Nom", "Implantation"],
    )

    # Filtre des données sur la commune de notre étude : Neuily-Sur-Seine
    df_station_clean = df_station[df_station["Commune"] == "NEUILLY-SUR-SEINE"]

    # chemin d'acces au fichier exporté
    output_path = "output/air_measurement_datas_clean/"

    # Export du dataframe nettoyé
    df_station_clean.to_csv(
        output_path + "transf_station.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    process_station()
