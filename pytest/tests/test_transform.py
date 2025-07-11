from etl.transform import transform_data
from etl.extract import extract_data
import pandas as pd
import pytest
from sqlalchemy import create_engine
from config.settings import DATABASE_URL
@pytest.fixture
def test_engine():
    return create_engine(DATABASE_URL)
def test_transform_data(test_engine, caplog):
    caplog.set_level("INFO")
    bd_source, bd_target = extract_data(test_engine)
    transformed_df = transform_data(bd_source, bd_target)

    # ✅ Tester que le résultat est bien un DataFrame 
    assert isinstance(transformed_df, pd.DataFrame)

    # ✅ Tester que les colonnes attendues sont présentes
    expected_columns = ['snapshot_date', 'id', 'datenaissance', 'age']
    assert all(col in transformed_df.columns for col in expected_columns)
    # ✅ Tester que les âges sont calculés correctement
    if not transformed_df.empty:
        assert all(transformed_df['age'] >= 0)
    # ✅ Tester qu'on a bien loggé un message d'info
    assert "Début de la transformation des données..." in caplog.text
    assert "Fusion réalisée" in caplog.text or "0 lignes traitées" in caplog.text
    assert "Calcul des âges terminé avec succès." in caplog.text
    # (Optionnel) Tester le contenu du DataFrame si besoin
    if not bd_source.empty:
        assert "id" in bd_source.columns
        assert "datenaissance" in bd_source.columns
        assert not transformed_df['datenaissance'].isnull().any()
        assert not transformed_df['age'].isnull().any()
        assert not transformed_df['snapshot_date'].isnull().any()
        assert transformed_df['snapshot_date'].dtype == 'datetime64[ns]'
        assert transformed_df['datenaissance'].dtype == 'datetime64[ns]'
        assert transformed_df['age'].dtype == 'int64'
        assert transformed_df['id'].dtype == 'int64'
        assert transformed_df['age'].apply(lambda x: isinstance(x, int) or pd.isnull(x)).all()
        assert transformed_df['snapshot_date'].apply(lambda x: isinstance(x, pd.Timestamp)).all()
        assert transformed_df['datenaissance'].apply(lambda x: isinstance(x, pd.Timestamp) or pd.isnull(x)).all()
        assert transformed_df['id'].apply(lambda x: isinstance(x, int)).all()
        assert transformed_df['id'].notnull().all()
        assert transformed_df['datenaissance'].notnull().all() or transformed_df['age'].isnull().all()
        assert transformed_df['snapshot_date'].notnull().all()
        assert transformed_df['age'].apply(lambda x: x >= 0 if pd.notnull(x) else True).all()
        assert transformed_df['datenaissance'].apply(lambda x: pd.isnull(x) or isinstance(x, pd.Timestamp)).all()
        assert transformed_df['snapshot_date'].apply(lambda x: isinstance(x, pd.Timestamp)).all()
        assert transformed_df['age'].apply(lambda x: isinstance(x, int) or pd.isnull(x)).all()
        assert transformed_df['id'].apply(lambda x: isinstance(x, int)).all()
        assert transformed_df['id'].notnull().all()
        assert transformed_df['datenaissance'].notnull().all() or transformed_df['age'].isnull().all()
        assert transformed_df['snapshot_date'].notnull().all()