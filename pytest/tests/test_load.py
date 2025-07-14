import pytest
import pandas as pd
from sqlalchemy import create_engine, text
from etl.load import load_data
from config.settings import DATABASE_URL

@pytest.fixture
def engine():
    return create_engine(DATABASE_URL)

def test_load_data(engine, caplog):
    caplog.set_level("INFO")

    # 1. Préparation : insérer une ligne temporaire de test dans target_table
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO target_table (id, snapshot_date)
            VALUES (9999, '2025-01-01')
            ON CONFLICT DO NOTHING
        """))

    # 2. Création d’un DataFrame à charger
    test_data = pd.DataFrame([{
        "id": 9999,
        "snapshot_date": pd.to_datetime("2025-01-01"),
        "datenaissance": pd.to_datetime("1990-01-01"),
        "age": 35
    }])

    # 3. Chargement
    load_data(engine, test_data)

    # 4. Vérification dans la base
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT datenaissance, age FROM target_table WHERE id = 9999 AND snapshot_date = '2025-01-01'
        """)).fetchone()
        assert result is not None
        assert result[0] == pd.to_datetime("1990-01-01")
        assert result[1] == 35

    # 5. Vérification des logs
    assert "Début du chargement des données" in caplog.text
    assert "1 lignes mises à jour" in caplog.text
