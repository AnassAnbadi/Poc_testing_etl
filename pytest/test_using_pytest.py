import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import pytest
import numpy as np
from datetime import date
from sqlalchemy import create_engine, text

# ⚙️ Configuration
def pytest_configure():
    DATABASE_URL = "postgresql://postgres:root@localhost:5432/postgres"
    # ✅ Connexion SQLAlchemy (si besoin)
    engine = create_engine(DATABASE_URL)
    return engine
 
bd_source=pd.read_sql("SELECT * FROM source_table", con=pytest_configure())
bd_target=pd.read_sql("SELECT * FROM target_table", con=pytest_configure())

print(bd_source.head())
print(bd_source.info())

print(bd_target.head())
print(bd_target.info())
# 🧪 Test avec pytest
@pytest.mark.parametrize("source_table, target_table", [
    ("source_table", "target_table"),
])
def test_data_integrity(source_table, target_table):
    # Récupération des données source et cible
    source_df = pd.read_sql(f"SELECT * FROM {source_table}", con=pytest_configure())
    target_df = pd.read_sql(f"SELECT * FROM {target_table}", con=pytest_configure())
    # Vérification de l'intégrité des données
    assert not source_df.empty, "La table source est vide"
    assert not target_df.empty, "La table cible est vide"
    # assert set(source_df.columns) == set(target_df.columns), "Les colonnes des tables source et cible ne correspondent pas"

    # Vérification des données spécifiques
    assert source_df['datenaissance'].dtype == 'datetime64[ns]', "Le type de la colonne 'datenaissance' dans la source n'est pas datetime"
    assert target_df['snapshot_date'].dtype == 'datetime64[ns]', "Le type de la colonne 'snapshot_date' dans la cible n'est pas datetime"
# 🗄️ Configuration de la base de données
    assert source_df['datenaissance'].notnull().all(), "La colonne 'datenaissance' dans la source contient des valeurs nulles"
    assert target_df['snapshot_date'].notnull().all(), "La colonne 'snapshot_date' dans la cible contient des valeurs nulles"
    assert source_df['datenaissance'].min() >= date(1900, 1, 1), "La colonne 'datenaissance' dans la source contient des dates antérieures à 1900"
    assert target_df['snapshot_date'].min() >= date(1900, 1, 1), "La colonne 'snapshot_date' dans la cible contient des dates antérieures à 1900"
    assert source_df['datenaissance'].max() <= date.today(), "La colonne 'datenaissance' dans la source contient des dates futures"
    assert target_df['snapshot_date'].max() <= date.today(), "La colonne 'snapshot_date' dans la cible contient des dates futures"
    # Vérification des valeurs uniques
    assert source_df['id'].is_unique, "La colonne 'id' dans la source n'est pas unique"
