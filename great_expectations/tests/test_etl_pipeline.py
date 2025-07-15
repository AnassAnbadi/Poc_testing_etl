# import pytest
# import pandas as pd
# from datetime import date
# from etl.extract import DataExtractor
# from etl.transform import DataTransformer
# from etl.load import DataLoader
# from etl.utils import calculate_age, get_table_row_count
# from config.settings import settings

# class TestETLPipeline:
    
#     def test_calculate_age_function(self):
#         """Test de la fonction de calcul d'âge"""
#         birth_date = date(1990, 5, 15)
#         snapshot_date = date(2024, 1, 15)
        
#         expected_age = 33
#         actual_age = calculate_age(birth_date, snapshot_date)
        
#         assert actual_age == expected_age
    
#     def test_calculate_age_birthday_not_passed(self):
#         """Test calcul d'âge quand l'anniversaire n'est pas encore passé"""
#         birth_date = date(1990, 6, 15)
#         snapshot_date = date(2024, 1, 15)
        
#         expected_age = 33
#         actual_age = calculate_age(birth_date, snapshot_date)
        
#         assert actual_age == expected_age
    
#     def test_calculate_age_future_birth_date(self):
#         """Test avec date de naissance dans le futur"""
#         birth_date = date(2025, 1, 1)
#         snapshot_date = date(2024, 1, 15)
        
#         expected_age = 0
#         actual_age = calculate_age(birth_date, snapshot_date)
        
#         assert actual_age == expected_age
    
#     def test_data_extraction(self, clean_tables, sample_source_data):
#         """Test d'extraction des données"""
#         # Insérer des données de test
#         loader = DataLoader()
#         with loader.DatabaseConnection() as conn:
#             cursor = conn.cursor()
#             for _, row in sample_source_data.iterrows():
#                 cursor.execute(
#                     f"INSERT INTO {settings.etl.source_table} (id, datenaissance) VALUES (%s, %s)",
#                     (row['id'], row['datenaissance'])
#                 )
        
#         # Tester l'extraction
#         extractor = DataExtractor()
#         extracted_data = extractor.extract_source_data()
        
#         assert len(extracted_data) == len(sample_source_data)
#         assert list(extracted_data.columns) == ['id', 'datenaissance']
    
#     def test_data_transformation(self):
#         """Test de transformation des données"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data()
        
#         # Vérifier la structure
#         expected_columns = ['snapshot_date', 'id', 'datenaissance', 'age']
#         assert list(transformed_data.columns) == expected_columns
        
#         # Vérifier que toutes les lignes ont la même snapshot_date
#         # assert all(transformed_data['snapshot_date'] == snapshot_date)
        
#         # Vérifier que les âges sont calculés correctement
#         # for _, row in transformed_data.iterrows():
#         #     expected_age = calculate_age(row['datenaissance'], snapshot_date)
#         #     assert row['age'] == expected_age
    
#     def test_data_validation(self, sample_source_data, snapshot_date):
#         """Test de validation des données transformées"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data()
        
#         is_valid = transformer.validate_transformed_data(transformed_data)
#         assert is_valid is True
    
#     def test_data_loading(self):
#         """Test de chargement des données"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data()
        
#         loader = DataLoader()
#         result = loader.load_data(transformed_data)
        
#         assert result is True
        
#         # Vérifier que les données ont été insérées
#         # row_count = get_table_row_count(settings.etl.target_table)
#         # assert row_count == len(sample_source_data)
    
#     def test_complete_etl_pipeline(self, clean_tables, sample_source_data, snapshot_date):
#         """Test du pipeline ETL complet"""
#         # Setup: Insérer des données source
#         loader = DataLoader()
#         with loader.DatabaseConnection() as conn:
#             cursor = conn.cursor()
#             for _, row in sample_source_data.iterrows():
#                 cursor.execute(
#                     f"INSERT INTO {settings.etl.source_table} (id, datenaissance) VALUES (%s, %s)",
#                     (row['id'], row['datenaissance'])
#                 )
        
#         # Extract
#         extractor = DataExtractor()
#         extracted_data = extractor.extract_source_data()
        
#         # Transform
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data(extracted_data, snapshot_date)
        
#         # Validate
#         is_valid = transformer.validate_transformed_data(transformed_data)
#         assert is_valid is True
        
#         # Load
#         load_result = loader.load_data(transformed_data)
#         assert load_result is True
        
#         # Verify final result
#         final_count = get_table_row_count(settings.etl.target_table)
#         assert final_count == len(sample_source_data)

# class TestDataQuality:
    
#     def test_no_duplicate_ids_per_snapshot(self, clean_tables, sample_source_data, snapshot_date):
#         """Test qu'il n'y a pas de doublons d'ID par snapshot"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data(sample_source_data, snapshot_date)
        
#         loader = DataLoader()
#         loader.load_data(transformed_data)
        
#         # Vérifier l'absence de doublons
#         with loader.DatabaseConnection() as conn:
#             cursor = conn.cursor()
#             cursor.execute(f"""
#                 SELECT snapshot_date, id, COUNT(*) as count
#                 FROM {settings.etl.target_table}
#                 GROUP BY snapshot_date, id
#                 HAVING COUNT(*) > 1
#             """)
#             duplicates = cursor.fetchall()
            
#         assert len(duplicates) == 0, f"Doublons détectés: {duplicates}"
    
#     def test_age_consistency(self, clean_tables, sample_source_data, snapshot_date):
#         """Test de cohérence des âges calculés"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data(sample_source_data, snapshot_date)
        
#         loader = DataLoader()
#         loader.load_data(transformed_data)
        
#         # Vérifier la cohérence des âges
#         with loader.DatabaseConnection() as conn:
#             cursor = conn.cursor()
#             cursor.execute(f"""
#                 SELECT id, datenaissance, age, snapshot_date
#                 FROM {settings.etl.target_table}
#             """)
#             results = cursor.fetchall()
            
#         for row in results:
#             id_val, birth_date, age, snapshot_dt = row
#             expected_age = calculate_age(birth_date, snapshot_dt)
#             assert age == expected_age, f"Âge incohérent pour ID {id_val}: attendu {expected_age}, obtenu {age}"
    
#     def test_data_completeness(self, clean_tables, sample_source_data, snapshot_date):
#         """Test de complétude des données"""
#         transformer = DataTransformer()
#         transformed_data = transformer.transform_data(sample_source_data, snapshot_date)
        
#         loader = DataLoader()
#         loader.load_data(transformed_data)
        
#         # Vérifier qu'aucune donnée n'est manquante
#         with loader.DatabaseConnection() as conn:
#             cursor = conn.cursor()
#             cursor.execute(f"""
#                 SELECT 
#                     COUNT(*) as total_rows,
#                     COUNT(snapshot_date) as snapshot_date_count,
#                     COUNT(id) as id_count,
#                     COUNT(datenaissance) as datenaissance_count,
#                     COUNT(age) as age_count
#                 FROM {settings.etl.target_table}
#             """)
#             result = cursor.fetchone()
            
#         total, snapshot_count, id_count, birth_count, age_count = result
#         assert total == snapshot_count == id_count == birth_count == age_count, \
#             "Données manquantes détectées"
