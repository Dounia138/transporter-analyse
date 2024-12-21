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

insert_tables_sql = """
-- INSERT DIM_STATION --
INSERT INTO dim_station 
SELECT
	a.id_station,
	b.id_commune,
	a.nom_station,
	a.type_implant
FROM stg_station a
LEFT JOIN dim_commune b
	on (a.nom_commune = b.nom_commune)


-- INSERT FACT_TRANSPORT --

INSERT INTO fact_transport
SELECT
	b.id_transport,
	c.id_commune,
	a.nb_transport,
	a.year
FROM stg_transport a
LEFT JOIN dim_transport b
	ON (a.nom_transport = b.nom_transport)
LEFT JOIN dim_commune c
	ON (UPPER(a.nom_commune) = c.nom_commune)

-- INSERT FACT_DEMOGRAPHIE --

INSERT INTO fact_demographie
SELECT 
	b.id_commune,
	unite_legal_2021,
	pop22,
	dens1876_2021,
	part_construction_entreprise_2023
FROM stg_demographie a
LEFT JOIN dim_commune b
	ON (UPPER(a.nom_commune) = b.nom_commune)


-- INSERT FACT_AIR --

INSERT INTO fact_air
SELECT
	c.id_date,
	a.code_site,
	b.id_pollution,
	cast(valeur_brute as float) as valeur_brute,
	code_unite_mesure
FROM stg_air a
LEFT JOIN dim_pollution b 
	ON (a.nom_polluant = b.nom_polluant)
LEFT JOIN dim_date c 
	ON (a.date = c.date)
INNER JOIN dim_station d
	ON (a.code_site = d.id_station)
"""

cursor.execute(insert_tables_sql)

connection.commit()

cursor.close()
connection.close()

print("Tables créées avec succès dans la base de données.")
