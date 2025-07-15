# main.py
import logging
import sys
import subprocess
from datetime import date
from sqlalchemy import create_engine
from etl.alert import send_email_alert
import os


from config.settings import DATABASE_URL, ALERT_EMAIL, ALERT_PASSWORD
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

# Logger global
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("main")

def run_pipeline():
	logger.info("🚀 Lancement du pipeline ETL...")

	engine = create_engine(DATABASE_URL)

	try:
		# Étape 1 : Extraction
		source_df, target_df = extract_data(engine)

		# Étape 2 : Transformation
		transformed_df = transform_data(source_df, target_df)

		# Étape 3 : Chargement
		load_data(engine, transformed_df)

		logger.info("✅ Pipeline exécuté avec succès.")
	except Exception as e:
		logger.error(f"❌ Erreur dans le pipeline : {e}")
		sys.exit(1)


def run_tests():
	logger.info("🧪 Lancement des tests automatiques avec Pytest...")
	result = subprocess.run([
    "pytest",
    "-n", "8",  # 8 workers (ajustez selon votre CPU)
    "--dist=loadfile",  # Meilleure parallélisation
    "--disable-warnings",
    "--html=reports/test_report.html",
    "--self-contained-html",
    "--tb=native",  # Traces minimales
    "--no-header",  # Supprime le header inutile
    "--no-summary",  # Supprime le summary inutile
], capture_output=True, text=True)
	print(result.stdout)

	if result.returncode != 0:
		logger.error("❌ Des tests ont échoué !")

		# Envoi d'une alerte par email

		send_email_alert(
			subject="❌ Échec des tests post-ETL",
			body="Un ou plusieurs tests ont échoué lors du pipeline ETL.\n\nLogs :\n" + result.stdout,
			to_email="anass.anbadi@usms.ac.ma",
			from_email=ALERT_EMAIL,
			from_password=ALERT_PASSWORD
		)
		sys.exit(result.returncode)

	logger.info("✅ Tous les tests ont passé avec succès.")


if __name__ == "__main__":
	run_pipeline()
	run_tests()
