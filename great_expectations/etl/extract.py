import pandas as pd
from datetime import datetime
from etl.utils import DatabaseConnection, logger
from config.settings import settings

class DataExtractor:
    def __init__(self):
        self.source_table = settings.etl.source_table
        
    def extract_source_data(self) -> pd.DataFrame:
        """Extrait les données de la table source"""
        query = f"""
        SELECT id, datenaissance 
        FROM {self.source_table}
        WHERE datenaissance IS NOT NULL
        ORDER BY id
        """
        
        try:
            with DatabaseConnection() as conn:
                df = pd.read_sql_query(query, conn)
                logger.info(f"Extraction réussie: {len(df)} lignes extraites de {self.source_table}")
                return df
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction: {e}")
            raise
    
    def get_snapshot_date(self) -> datetime:
        """Retourne la date de snapshot (aujourd'hui par défaut)"""
        if settings.etl.snapshot_date:
            return datetime.strptime(settings.etl.snapshot_date, '%Y-%m-%d').date()
        return datetime.now().date()
    
    def extract_target_structure(self) -> pd.DataFrame:
        """Extrait la structure de la table target pour validation"""
        query = f"""
        SELECT snapshot_date, id, datenaissance, age 
        FROM {settings.etl.target_table}
        """
        
        try:
            with DatabaseConnection() as conn:
                df = pd.read_sql_query(query, conn)
                return df
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction de la structure target: {e}")
            raise
