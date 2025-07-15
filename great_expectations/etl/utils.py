import logging
from datetime import datetime, date
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import settings

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Engine global (tu peux aussi en créer un dans __main__)
engine: Engine = create_engine(settings.db.connection_string)

# ==========================
# 📦 Connexion DB (context manager)
# ==========================
class DatabaseConnection:
    def __init__(self):
        self.engine = engine
        self.connection = None

    def __enter__(self):
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


# ==========================
# 🧮 Calculs utilitaires
# ==========================
def calculate_age(birth_date: date, snapshot_date: date) -> int:
    """Calcule l'âge basé sur la date de naissance et la date de snapshot"""
    if birth_date > snapshot_date:
        return 0

    age = snapshot_date.year - birth_date.year
    if (snapshot_date.month, snapshot_date.day) < (birth_date.month, birth_date.day):
        age -= 1

    return max(0, age)


def validate_date(date_str: str) -> bool:
    """Valide le format de date YYYY-MM-DD"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# ==========================
# 🧪 Fonctions DB
# ==========================
def get_table_row_count(table_name: str) -> int:
    """Retourne le nombre de lignes dans une table"""
    with DatabaseConnection() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()  # ⬅️ retourne un entier


def execute_query(query: str, params: Optional[tuple] = None) -> List[tuple]:
    """Exécute une requête et retourne les résultats"""
    with DatabaseConnection() as conn:
        result = conn.execute(text(query), params or {})
        return result.fetchall()
