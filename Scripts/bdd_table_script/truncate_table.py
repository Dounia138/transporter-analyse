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

truncate_tables_sql = """
TRUNCATE TABLE dim_station CASCADE;
TRUNCATE TABLE dim_pollution CASCADE;
TRUNCATE TABLE dim_date CASCADE ;
TRUNCATE TABLE stg_air CASCADE ;
TRUNCATE TABLE stg_station CASCADE ;
TRUNCATE TABLE stg_transport CASCADE;
TRUNCATE TABLE stg_demographie CASCADE;
TRUNCATE TABLE dim_transport CASCADE;
TRUNCATE TABLE dim_commune CASCADE;
"""

cursor.execute(truncate_tables_sql)

connection.commit()

cursor.close()
connection.close()

print("Tables créées avec succès dans la base de données.")
