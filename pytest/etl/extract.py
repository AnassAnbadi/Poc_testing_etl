import logging
import pandas as pd

logger = logging.getLogger(__name__)

def extract_data(engine):
    try:
        logger.info("Lancement de l'extraction des données...")
        # Extraction des données de la table source
        bd_source = pd.read_sql("SELECT * FROM source_table", con=engine)
        bd_target = pd.read_sql("SELECT * FROM target_table", con=engine)
        logger.info(f"{len(bd_source)} lignes extraites pour bd_source.")
        logger.info(f"{len(bd_target)} lignes extraites pour bd_target.")
        return bd_source, bd_target
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction : {e}")
        return pd.DataFrame()  # retourne un DataFrame vide si erreur
