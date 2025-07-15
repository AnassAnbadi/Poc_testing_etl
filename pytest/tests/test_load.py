import pytest
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from etl.load import load_data
from config.settings import DATABASE_URL

@pytest.fixture
def engine():
    return create_engine(DATABASE_URL)

def test_load_data(engine, caplog):
    caplog.set_level("INFO")

    # 1. Nettoyage : supprimer la ligne si elle existe déjà
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM target_results
            WHERE id = 9999 AND snapshot_date = '2025-01-01'
        """))

    # 2. Création d’un DataFrame à charger (simule des données transformées)
    test_df = pd.DataFrame([{
        "id": 9999,
        "snapshot_date": pd.to_datetime("2025-01-01"),
        "datenaissance": pd.to_datetime("1990-01-01"),
        "age": 35
    }])

    # 3. Chargement avec la fonction ETL
    load_data(engine, test_df)

    # 4. Vérification dans la base que l'insertion a bien eu lieu
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, snapshot_date, datenaissance, age
            FROM target_results
            WHERE id = 9999 AND snapshot_date = '2025-01-01'
        """)).fetchone()

    assert result is not None, "Aucune ligne insérée dans target_results"
    assert result[0] == 9999
    assert pd.to_datetime(result[1]) == pd.to_datetime("2025-01-01")
    assert pd.to_datetime(result[2]) == pd.to_datetime("1990-01-01")
    assert result[3] == 35

    # 5. Vérification des logs
    assert "Début de l'insertion des données dans target_results" in caplog.text
    assert "1 lignes insérées dans target_results." in caplog.text


def test_load_data_multiple_rows(engine, caplog):
    caplog.set_level("INFO")

    # 1. Nettoyage des IDs utilisés pour le test
    test_ids = [8888, 8889, 8890]
    with engine.begin() as conn:
        conn.execute(
    text("""
        SELECT COUNT(*) FROM target_results
        WHERE id IN :ids
    """).bindparams(
        bindparam("ids", expanding=True)
    ),
    {"ids": test_ids}
)


    # 2. Créer un DataFrame avec plusieurs lignes, dont certaines incomplètes
    test_df = pd.DataFrame([
        {
            "id": 8888,
            "snapshot_date": pd.to_datetime("2025-01-01"),
            "datenaissance": pd.to_datetime("1990-01-01"),
            "age": 35
        },
        {
            "id": 8889,
            "snapshot_date": pd.to_datetime("2025-01-01"),
            "datenaissance": None,   # 🔸 valeur manquante
            "age": None
        },
        {
            "id": 8890,
            "snapshot_date": pd.to_datetime("2025-01-01"),
            "datenaissance": pd.to_datetime("1980-05-05"),
            "age": 45
        }
    ])

    # 3. Charger dans la table
    load_data(engine, test_df)

    # 4. Vérifier que les 3 lignes ont bien été insérées
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM target_results
            WHERE id IN :ids
        """), {"ids": tuple(test_ids)}).scalar()

    assert result == 3, f"⛔ {result}/3 lignes seulement ont été insérées"

    # 5. Vérifier que les logs sont bien là
    assert "Début de l'insertion des données dans target_results" in caplog.text
    assert "3 lignes insérées dans target_results." in caplog.text

def test_target_results_not_empty(engine):
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM target_results")).scalar()
        assert count > 0, "⛔ La table target_results est vide"

def test_target_results_columns_are_valid(engine):
    df = pd.read_sql("SELECT * FROM target_results LIMIT 100", engine)

    expected_cols = ["id", "snapshot_date", "datenaissance", "age"]
    for col in expected_cols:
        assert col in df.columns, f"Colonne manquante : {col}"

    assert pd.api.types.is_integer_dtype(df["id"])
    assert pd.api.types.is_datetime64_any_dtype(df["snapshot_date"])
    assert pd.api.types.is_datetime64_any_dtype(df["datenaissance"]) or df["datenaissance"].isnull().all()
    assert pd.api.types.is_integer_dtype(df["age"]) or df["age"].isnull().all()
def test_target_results_age_validity(engine):
    df = pd.read_sql("SELECT age FROM target_results", engine)

    # Ne tester que les valeurs non nulles
    non_null_ages = df["age"].dropna()

    assert all(non_null_ages >= 0), "⛔ Certains âges sont négatifs"
    assert all(non_null_ages <= 120), "⛔ Certains âges sont irréalistes"


def test_target_results_id_unique_per_snapshot(engine):
    df = pd.read_sql("SELECT id, snapshot_date FROM target_results", engine)

    duplicates = df.duplicated(subset=["id", "snapshot_date"]).sum()
    assert duplicates == 0, f"⛔ {duplicates} doublons trouvés sur (id, snapshot_date)"
