import pandas as pd
from etl.utils import DatabaseConnection, logger
from config.settings import settings
from etl.transform import DataTransformer

from sqlalchemy import text

class DataLoader:
    def __init__(self):
        self.target_results_for_ge = DataTransformer().transform_data()
        self.target_table = settings.etl.target_table
        
    def load_data(self) -> bool:
        """Charge les données dans la table target"""
        try:
            with DatabaseConnection() as conn:
                insert_query = """
                INSERT INTO target_results_for_ge 
                (snapshot_date, id, datenaissance, age)
                VALUES (:snapshot_date, :id, :datenaissance, :age)
                """
                
                data_dicts = [
                    {
                        'snapshot_date': row['snapshot_date'],
                        'id': row['id'],
                        'datenaissance': row['datenaissance'],
                        'age': row['age']
                    }
                    for _, row in self.target_results_for_ge.iterrows()
                ]
                
                conn.execute(text(insert_query), data_dicts)
                conn.commit()  # selon implémentation
                
                logger.info(f"Chargement réussi: {len(data_dicts)} lignes insérées dans {self.target_table}")
                return True
                
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            raise

    def create_tables_if_not_exist(self):
        """Crée les tables si elles n'existent pas"""
        create_source_table = f"""
        CREATE TABLE IF NOT EXISTS {settings.etl.target_results_for_ge} (
            id SERIAL PRIMARY KEY,
            datenaissance DATE NOT NULL
        );target_table
        """
        
        create_target_table = f"""
        CREATE TABLE IF NOT EXISTS {self.target_table} (
            snapshot_date DATE NOT NULL,
            id INTEGER NOT NULL,
            datenaissance DATE,
            age INTEGER,
            PRIMARY KEY (snapshot_date, id)
        );
        """
        
        try:
            with DatabaseConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_source_table)
                cursor.execute(create_target_table)
                logger.info("Tables créées avec succès")
        except Exception as e:
            logger.error(f"Erreur lors de la création des tables: {e}")
            raise
    
    def insert_sample_data(self):
        """Insère des données d'exemple dans la table source"""
        sample_data = [
            ('1990-05-15',),
            ('1985-12-03',),
            ('2000-08-22',),
            ('1975-03-10',),
            ('1995-11-28',),
        ]
        
        insert_query = f"""
        INSERT INTO {settings.etl.source_table} (datenaissance)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        """
        
        try:
            with DatabaseConnection() as conn:
                cursor = conn.cursor()
                cursor.executemany(insert_query, sample_data)
                logger.info(f"Données d'exemple insérées: {cursor.rowcount} lignes")
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion des données d'exemple: {e}")
