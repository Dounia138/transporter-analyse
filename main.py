import os
import psycopg2
import subprocess
from dotenv import load_dotenv

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Liste des scripts de transformation (ETL)
TRANSFORM_SCRIPTS = [
    "transfo_scripts/clean_air_data.py",
    "transfo_scripts/clean_demographie_data.py",
    "transfo_scripts/clean_station_data.py",
    "transfo_scripts/clean_transport_data.py",
]

# Liste des scripts BDD pour la gestion des tables
BDD_TABLE_SCRIPTS = [
    "bdd_table_script/create_table.py",  # Crée les tables dans la BDD
    "bdd_table_script/truncate_table.py",  # Vide les tables avant l'insertion
]

# Liste des scripts ETL à exécuter pour insérer les données transformées dans la BDD
ETL_SCRIPTS = [
    "scripts/etl_demographie.py",
    "scripts/etl_air.py",
    "scripts/etl_station.py",
    "scripts/etl_transport.py",
]

# Script pour insérer les données dans les tables de faits
INSERT_FACT_TABLE_SCRIPTS = ["bdd_table_script/insert_fact_table.py"]

"""Exécute une liste de scripts Python"""


def execute_scripts(script_list):
    for script in script_list:
        print(f"Exécution de {script}...")
        subprocess.run(["python", script], check=True)
    print(f"Scripts {script_list} exécutés avec succès !")


"""Processus ETL complet avec gestion des tables"""


def run_etl():
    execute_scripts(BDD_TABLE_SCRIPTS[:1])
    execute_scripts(BDD_TABLE_SCRIPTS[1:])
    execute_scripts(TRANSFORM_SCRIPTS)
    execute_scripts(INSERT_FACT_TABLE_SCRIPTS)


if __name__ == "__main__":
    run_etl()
