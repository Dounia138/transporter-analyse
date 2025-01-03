# Importation des librairies
import os
import pandas as pd

# chemin d'acces aux fichiers sources
input_path = "input/transport_datas"


def process_transport():

    df_ref_ligne = pd.read_csv(
        os.path.join(input_path, "/GTFS/stop/stops.txt"), sep=",", usecols=["stop_name"]
    )
    df_transport = pd.read_csv(
        os.path.join(input_path, "/ile_de_france/referentiel_de_ligne/arrets.csv"),
        sep=";",
        usecols=["ArRName", "ArRTown", "ArRType"],
    )

    df_merge = df_ref_ligne.merge(
        df_transport, how="inner", left_on="stop_name", right_on="ArRName"
    )

    df_merge_clean = df_merge.drop_duplicates()

    df_transport_clean = df_transport_clean[["ArRType", "ArRTown"]].drop_duplicates()

    # Ajout de la varialbe "nombre de transport" avec pour valeur 1 représentant le nombre de ligne et la colonne annee avec pour valeur 2024 (l'année de référence du fichier)
    df_transport_clean["NombreDeTransport"] = 1
    df_transport_clean["Annee"] = 2024

    # chemin d'acces au fichier exporté
    output_path = "output/transport_datas_clean/"

    # Export du dataframe nettoyé
    df_transport_clean.to_csv(
        output_path + "transf_transport.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    process_transport()
