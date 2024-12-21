# Documentation du Projet

## Prérequis

Avant de commencer, assurez-vous que vous avez les prérequis suivants installés :

- Python 3.x
- PostgreSQL
- `pip` pour l'installation des dépendances

## Installation des Dépendances

Clonez le projet et installez les dépendances nécessaires avec la commande suivante :

git clone ‘https://github.com/Dounia138/transporter-analyse’
cd transporter-analyse
pip install -r requirements.txt

## Variables d'Environnement

Ce projet utilise un fichier `.env` pour stocker les informations sensibles telles que les paramètres de connexion à la base de données. Assurez-vous d'avoir ce fichier à la racine du projet avec les variables suivantes :

DB_HOST=<hôte_de_votre_base_de_données>
DB_NAME=<nom_de_votre_base_de_données>
DB_USER=<utilisateur_de_votre_base_de_données>
DB_PASSWORD=<mot_de_passe_de_votre_base_de_données>

## Exécution du Pipeline ETL

Ce projet automatise l'exécution des processus ETL (Extraction, Transformation, et Chargement) dans une base de données PostgreSQL. Voici comment l'exécuter.

## Configuration de l'Environnement

Avant d'exécuter les scripts, configurez votre environnement en renseignant les informations de votre base de données dans le fichier `.env` à la racine du projet. Par exemple :

DB_HOST=localhost
DB_NAME=my_database
DB_USER=my_user
DB_PASSWORD=my_password

## Exécution des Scripts ETL

Une fois les dépendances installées et le fichier `.env` configuré, vous pouvez exécuter le pipeline ETL avec la commande suivante :

python main.py

Ou bien exécutez chaque script ETL séparément, par exemple :

python scripts/etl_demographie.py
python scripts/etl_air.py
python scripts/etl_station.py
python scripts/etl_transport.py

Cela effectuera les actions suivantes :

1. **Vider les tables** : Avant l'exécution des ETL, les tables dans la base de données seront vidées pour éviter les doublons.  
2. **Exécution des scripts ETL** : Tous les scripts ETL seront exécutés dans l'ordre, et les données seront transformées et insérées dans la base de données PostgreSQL.

## Exécution des Tests

Les tests pour vérifier le bon fonctionnement du projet sont réalisés à l'aide de `pytest`. Pour exécuter les tests, vous pouvez utiliser la commande suivante :

pytest

Cela exécutera tous les tests présents dans le répertoire `tests/` et vous donnera un rapport détaillé sur les résultats. Si tous les tests passent, cela signifie que les scripts ETL et les fonctions associées fonctionnent correctement.

# Structure du Code

Voici un aperçu des principales fonctions dans le projet :

- **`truncate_tables()`** : Vide les tables dans la base de données avant chaque exécution du pipeline ETL pour éviter les doublons.  
- **`run_etl()`** : Exécute tous les scripts ETL après avoir vidé les tables. Cette fonction appelle les scripts ETL dans un ordre défini et insère les données traitées dans la base de données PostgreSQL.  
- **`insert_fact_table()`** : Insère les données de faits dans la table correspondante dans la base de données après les transformations.