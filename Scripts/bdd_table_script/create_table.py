import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


connection = psycopg2.connect(
    host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)

cursor = connection.cursor()

create_tables_sql = """
-- stg_air --
CREATE TABLE stg_air (
    date date,
    code_site varchar(255),
    nom_polluant varchar(255),
    valeur_brute varchar(255),
    code_unite_mesure varchar(255)
);

-- stg_station --
CREATE TABLE IF NOT EXISTS stg_station (
    id_station varchar(50),
    nom_station VARCHAR(50),
	nom_commune VARCHAR(50),
	type_implant VARCHAR(50)
);

-- stg_transport --
CREATE TABLE IF NOT EXISTS stg_transport (
    nom_transport varchar(50),
    nom_commune VARCHAR(50),
	nb_transport int,
	year int
);

-- stg_demographie --
CREATE TABLE IF NOT EXISTS stg_demographie (
    nom_commune VARCHAR(50),
	Unite_legal_2021 int,
	pop22 int,
	dens1876_2021 float,
	part_construction_entreprise_2023 float
);

-- DIM_TRANSPORT --
CREATE TABLE IF NOT EXISTS dim_transport (
    id_transport SERIAL PRIMARY KEY,
    nom_transport VARCHAR(50)
);

-- dim_commune --
CREATE TABLE IF NOT EXISTS dim_commune (
    id_commune SERIAL PRIMARY KEY,
    nom_commune VARCHAR(50),
	lont varchar(255),
	lat varchar(255)
);

-- dim_pollution -- 
CREATE TABLE IF NOT EXISTS dim_pollution (
    id_pollution SERIAL PRIMARY KEY,
    nom_polluant varchar(50)
);

-- dim_date --
CREATE TABLE IF NOT EXISTS dim_date (
    id_date int PRIMARY KEY,
    date date,
	year int,
	month int,
	quarter int,
	day int
);


-- dim_station --
CREATE TABLE IF NOT EXISTS dim_station (
    id_station varchar(255) PRIMARY KEY,
    id_commune int,
	nom_station varchar(255),
	type_implant varchar(255),
	FOREIGN KEY (id_commune) REFERENCES dim_commune(id_commune)
);

-- fact_transport --
CREATE TABLE IF NOT EXISTS fact_transport (
    id_transport int,
    id_commune int,
	nb_transport int,
	code_annee int,
	FOREIGN KEY (id_transport) REFERENCES dim_transport(id_transport),
	FOREIGN KEY (id_commune) REFERENCES dim_commune(id_commune)
);

-- fact_demographie --
CREATE TABLE IF NOT EXISTS fact_demographie (
    id_commune int,
	unite_legal_2021 int,
	pop22 int,
	dens1876_2021 float,
	part_construction_entreprise_2023 float,
	FOREIGN KEY (id_commune) REFERENCES dim_commune(id_commune)
);

-- fact_air --
CREATE TABLE IF NOT EXISTS fact_air (
    id_date int,
	id_site varchar(255),
	id_pollution int,
	valeur_brute float,
	code_unite_mesure varchar(255),
	FOREIGN KEY (id_date) REFERENCES dim_date(id_date),
	FOREIGN KEY (id_site) REFERENCES dim_station(id_station),
	FOREIGN KEY (id_pollution) REFERENCES dim_pollution(id_pollution)
);
"""

cursor.execute(create_tables_sql)

connection.commit()

cursor.close()
connection.close()

print("Tables créées avec succès dans la base de données.")
