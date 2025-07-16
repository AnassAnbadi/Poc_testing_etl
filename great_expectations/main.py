#!/usr/bin/env python3
"""
Pipeline ETL principal avec validation pytest et Great Expectations
"""

import sys
import os
import logging
from datetime import datetime, date
import argparse

# Ajouter le répertoire courant au path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader
from ge_runner.run_validation import GreatExpectationsRunner
from config.settings import settings

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ETLPipeline:
	def __init__(self, snapshot_date: date = None):
		self.extractor = DataExtractor()
		self.transformer = DataTransformer()
		self.loader = DataLoader()
		self.ge_runner = GreatExpectationsRunner()
		self.snapshot_date = snapshot_date or datetime.now().date()
		
	def setup_database(self):
		"""Initialise la base de données avec les tables et données d'exemple"""
		logger.info("🔧 Configuration de la base de données...")
		try:
			self.loader.create_tables_if_not_exist()
			self.loader.insert_sample_data()
			logger.info("✅ Base de données configurée avec succès")
			return True
		except Exception as e:
			logger.error(f"❌ Erreur lors de la configuration de la base de données: {e}")
			return False
	
	def run_etl(self):
		"""Exécute le pipeline ETL complet"""
		logger.info(f"🚀 Démarrage du pipeline ETL pour la date: {self.snapshot_date}")
		
		try:
			# 1. Extract
			logger.info("📥 Phase d'extraction...")
			source_data = self.extractor.extract_source_data()
			if source_data.empty:
				logger.warning("⚠️ Aucune donnée à traiter")
				return False
			
			# 2. Transform
			logger.info("🔄 Phase de transformation...")
			transformed_data = self.transformer.transform_data()
			
			# 3. Validate transformation
			logger.info("✅ Validation des données transformées...")
			if not self.transformer.validate_transformed_data(transformed_data):
				logger.error("❌ Validation des données transformées échouée")
				return False
			
			# 4. Load
			logger.info("📤 Phase de chargement...")
			if not self.loader.load_data():
				logger.error("❌ Chargement des données échoué")
				return False
			
			logger.info("✅ Pipeline ETL terminé avec succès")
			return True
			
		except Exception as e:
			logger.error(f"❌ Erreur dans le pipeline ETL: {e}")
			return False
	
	def run_great_expectations_validation(self):
		"""Exécute les validations Great Expectations"""
		logger.info("🔍 Démarrage des validations Great Expectations...")
		
		try:
			validation_success = self.ge_runner.validate_latest_data()
			docs_success = self.ge_runner.create_data_docs()
			
			if validation_success and docs_success:
				logger.info("✅ Validations Great Expectations réussies")
				return True
			else:
				logger.error("❌ Validations Great Expectations échouées")
				return False
				
		except Exception as e:
			logger.error(f"❌ Erreur lors des validations Great Expectations: {e}")
			return False
	
	def run_complete_pipeline(self):
		"""Exécute le pipeline complet avec validations"""
		logger.info("🎯 Démarrage du pipeline ETL complet")
		
		# 1. Setup database
		# if not self.setup_database():
		# 	return False
		
		# 2. Run ETL
		if not self.run_etl():
			return False
		
		# 3. Run Great Expectations validation
		if not self.run_great_expectations_validation():
			logger.warning("⚠️ Validations Great Expectations échouées, mais ETL terminé")
		
		logger.info("🎉 Pipeline complet terminé!")
		return True

def main():
	"""Fonction principale"""
	parser = argparse.ArgumentParser(description='Pipeline ETL avec validations')
	parser.add_argument('--snapshot-date', type=str, help='Date de snapshot (YYYY-MM-DD)')
	parser.add_argument('--setup-only', action='store_true', help='Configurer seulement la base de données')
	parser.add_argument('--etl-only', action='store_true', help='Exécuter seulement l\'ETL')
	parser.add_argument('--validate-only', action='store_true', help='Exécuter seulement les validations')
	
	args = parser.parse_args()
	
	# Parse snapshot date
	snapshot_date = None
	if args.snapshot_date:
		try:
			snapshot_date = datetime.strptime(args.snapshot_date, '%Y-%m-%d').date()
		except ValueError:
			logger.error("❌ Format de date invalide. Utilisez YYYY-MM-DD")
			return 1
	
	# Initialize pipeline
	pipeline = ETLPipeline(snapshot_date)
	
	# Execute based on arguments
	if args.setup_only:
		success = pipeline.setup_database()
	elif args.etl_only:
		success = pipeline.run_etl()
	elif args.validate_only:
		success = pipeline.run_great_expectations_validation()
	else:
		success = pipeline.run_complete_pipeline()
	
	return 0 if success else 1

if __name__ == "__main__":
	exit_code = main()
	sys.exit(exit_code)
