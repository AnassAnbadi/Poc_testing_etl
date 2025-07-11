from etl.utils import calculate_age
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_data(source_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remplit les colonnes 'datenaissance' et 'age' dans target_df
    à partir des données de source_df en se basant sur 'id'.
    """
    logger.info("Début de la transformation des données...")

    try:
        # Conversion des dates
        source_df['datenaissance'] = pd.to_datetime(source_df['datenaissance'],
            errors='coerce')
        target_df['snapshot_date'] = pd.to_datetime(target_df['snapshot_date'],
            errors='coerce')

        # Fusion
        merged_df = target_df.merge(
            source_df[['id', 'datenaissance']],
            on='id',
            how='left',
            suffixes=('', '_source')
        )

        logger.info(f"Fusion réalisée : {len(merged_df)} lignes traitées.")

        # Calcul des âges
        merged_df['age'] = merged_df.apply(
            lambda row: calculate_age(row['datenaissance'], row['snapshot_date']),
            axis=1
        )

        logger.info("Calcul des âges terminé avec succès.")
        return merged_df[['snapshot_date', 'id', 'datenaissance', 'age']]

    except Exception as e:
        logger.error(f"Erreur lors de la transformation : {e}")
        return pd.DataFrame()  # retourne un DataFrame vide si erreur
