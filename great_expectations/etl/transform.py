import pandas as pd
from datetime import datetime, date
from etl.utils import calculate_age, logger
from etl.extract import DataExtractor

class DataTransformer:
    def __init__(self):
        self.extractor = DataExtractor()
        
    def transform_data(self) -> pd.DataFrame:
        """Transforme les données source pour la table target"""
        try:
            # Créer le DataFrame target et source
            target_df = self.extractor.extract_target_structure()
            source_df = self.extractor.extract_source_data()
            
            merged_df = target_df[['snapshot_date', 'id', 'age']].merge(
            source_df[['id', 'datenaissance']],
            on='id',
            how='left',
            suffixes=('', '_source')
            )
            merged_df['age'] = merged_df.apply(
            lambda row: calculate_age(row['datenaissance'], row['snapshot_date']),
            axis=1
        )  # Utilisation de Int64 pour gérer les NaN
            logger.info(f"Transformation réussie: {len(merged_df)} lignes transformées")
            return merged_df[['snapshot_date', 'id', 'datenaissance', 'age']]
            
        except Exception as e:
            logger.error(f"Erreur lors de la transformation: {e}")
            raise
    
    def validate_transformed_data(self, df: pd.DataFrame) -> bool:
        """Valide les données transformées"""
        validations = []
        
        # Vérifier les colonnes requises
        required_columns = ['snapshot_date', 'id', 'datenaissance', 'age']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes: {missing_columns}")
            return False
        
        # Vérifier les valeurs nulles
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Valeurs nulles détectées: {null_counts[null_counts > 0].to_dict()}")
        
        # Vérifier les âges négatifs
        negative_ages = df[df['age'] < 0]
        if not negative_ages.empty:
            logger.error(f"Âges négatifs détectés: {len(negative_ages)} lignes")
            return False
        
        # Vérifier les âges supérieurs à 150 ans
        old_ages = df[df['age'] > 150]
        if not old_ages.empty:
            logger.warning(f"Âges supérieurs à 150 ans: {len(old_ages)} lignes")
        
        logger.info("Validation des données transformées réussie")
        return True
