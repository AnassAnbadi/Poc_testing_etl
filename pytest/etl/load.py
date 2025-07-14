import logging
import pandas as pd

logger = logging.getLogger(__name__)

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def load_data(engine, transformed_df: pd.DataFrame):
    """
    Insère les données transformées dans la table `target_results`
    même si certaines valeurs sont nulles ou anormales.
    """
    logger.info("Début de l'insertion des données dans target_results...")

    if transformed_df.empty:
        logger.warning("Aucune donnée à insérer.")
        return

    try:
        transformed_df.to_sql(
            name="target_results",
            con=engine,
            if_exists="append",
            index=False
        )
        logger.info(f"{len(transformed_df)} lignes insérées dans target_results.")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion : {e}")
        raise
