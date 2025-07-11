# tests/test_extract.py
import pandas as pd
import pytest
from sqlalchemy import create_engine
from config.settings import DATABASE_URL
from etl.extract import extract_data

@pytest.fixture
def test_engine():
    return create_engine(DATABASE_URL)

def test_extract_data_returns_dataframe(test_engine, caplog):
    caplog.set_level("INFO")

    bd_source, bd_target = extract_data(test_engine)

    # ✅ Tester que le résultat est bien un DataFrame
    assert isinstance(bd_source, pd.DataFrame)
    assert isinstance(bd_target, pd.DataFrame)

    # ✅ Tester qu'on a bien loggé un message d'info
    assert "Lancement de l'extraction des données..." in caplog.text
    assert "lignes extraites" in caplog.text or "0 lignes extraites" in caplog.text

    # (Optionnel) Tester le contenu du DataFrame si besoin
    if not bd_source.empty:
        assert "id" in bd_source.columns
        assert "datenaissance" in bd_source.columns
