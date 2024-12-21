# Importation des librairies
import os
import pandas as pd

# chemin d'acces aux fichiers sources
input_path = "input/demographie_construct_datas"


def process_demographie():

    # chargement et selection des données démographique dans le dataframe
    df_demographie = pd.read_csv(
        os.path.join(input_path, "/demographie.csv"), sep=";", skiprows=2
    )

    # Filtre des données sur la commune de notre étude : Neuily-Sur-Seine
    df_demographie_clean = df_demographie[
        df_demographie["Libellé"] == "Neuilly-sur-Seine"
    ].drop_duplicates()

    # chemin d'acces au fichier exporté
    output_path = "output/demographie_datas_clean/"

    # Export du dataframe nettoyé
    df_demographie_clean.to_csv(
        output_path + "transf_demographie.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    process_demographie()
