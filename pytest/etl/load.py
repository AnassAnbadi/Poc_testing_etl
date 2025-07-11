from etl.extract import extract_data
from etl.utils import calculate_age
import pandas as pd

def transform_data(source_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    # Exemple de transformation : ajouter une colonne 'snapshot_date' à la table cible
    bd_target['snapshot_date'] = pd.to_datetime('now')
def enrich_target_with_age_and_birthdate(source_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remplit les colonnes 'datenaissance' et 'age' dans target_df
    à partir des données de source_df en se basant sur 'id'.
    """
    # On fusionne les deux DataFrames sur 'id'
    merged_df = target_df.merge(
        source_df[['id', 'datenaissance']],
        on='id',
        how='left',
        suffixes=('', '_source')
    )

    # Calcul de l'âge pour chaque ligne
    merged_df['age'] = merged_df.apply(
        lambda row: calculate_age(row['datenaissance'], pd.to_datetime(row['snapshot_date'])),
        axis=1
    )

    return merged_df[['snapshot_date', 'id', 'datenaissance', 'age']]
