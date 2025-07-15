import pytest
import psycopg2
import pandas as pd
from datetime import date, datetime
from config.settings import settings
from etl.utils import DatabaseConnection
from etl.load import DataLoader

@pytest.fixture(scope="session")
def db_connection():
    """Fixture pour la connexion à la base de données de test"""
    try:
        conn = psycopg2.connect(settings.db.connection_string)
        yield conn
    finally:
        conn.close()

@pytest.fixture(scope="function")
def clean_tables():
    """Nettoie les tables avant et après chaque test"""
    with DatabaseConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {settings.etl.target_table}")
        cursor.execute(f"DELETE FROM {settings.etl.source_table}")
    
    yield
    
    with DatabaseConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {settings.etl.target_table}")
        cursor.execute(f"DELETE FROM {settings.etl.source_table}")

@pytest.fixture
def sample_source_data():
    """Données d'exemple pour les tests"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'datenaissance': [
            date(1990, 5, 15),
            date(1985, 12, 3),
            date(2000, 8, 22),
            date(1975, 3, 10),
            date(1995, 11, 28)
        ]
    })

@pytest.fixture
def snapshot_date():
    """Date de snapshot pour les tests"""
    return date(2024, 1, 15)

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Configure la base de données de test"""
    loader = DataLoader()
    loader.create_tables_if_not_exist()
