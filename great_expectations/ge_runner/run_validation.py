import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
import logging
from datetime import datetime
import sys
import os

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GreatExpectationsRunner:
    def __init__(self):
        context_root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "great_expectations"))
        self.context = gx.get_context(context_root_dir=context_root_dir)
        
    def run_checkpoint(self, checkpoint_name: str = "target_results_checkpoint"):
        try:
            logger.info(f"Démarrage de la validation avec le checkpoint: {checkpoint_name}")
            checkpoint = self.context.get_checkpoint(checkpoint_name)
            result = checkpoint.run()
            if result['success']:
                logger.info("✅ Toutes les validations ont réussi!")
                self._log_validation_details(result)
                return True
            else:
                logger.error("❌ Certaines validations ont échoué!")
                self._log_validation_details(result)
                return False
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du checkpoint: {e}")
            return False

    def _log_validation_details(self, result):
        for _, validation_result in result["run_results"].items():
            vr = validation_result["validation_result"]
            suite_name = vr.get("meta", {}).get("expectation_suite_name", "Unknown")
            success_count = vr.get("statistics", {}).get("successful_expectations", 0)
            total_count = vr.get("statistics", {}).get("evaluated_expectations", 0)
            logger.info(f"📘 Suite: {suite_name}")
            logger.info(f"✅ Expectations réussies: {success_count}/{total_count}")
            if not vr.get("success", True):
                for result_item in vr.get("results", []):
                    if not result_item.get("success", True):
                        expectation_type = result_item.get("expectation_config", {}).get("expectation_type", "inconnue")
                        logger.error(f"❌ Échec: {expectation_type}")
                        unexpected_values = result_item.get("result", {}).get("partial_unexpected_list", [])
                        if unexpected_values:
                            logger.error(f"   ⚠️ Valeurs inattendues (extrait): {unexpected_values[:5]}")

    def create_data_docs(self):
        try:
            self.context.build_data_docs()
            logger.info("📊 Documentation des données générée avec succès")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la génération de la documentation: {e}")
            return False

    def validate_latest_data(self):
        return self.run_checkpoint("target_results_checkpoint")


def main():
    """Fonction principale pour exécuter les validations"""
    runner = GreatExpectationsRunner()
    
    logger.info("🚀 Démarrage des validations Great Expectations")
    
    # Exécuter les validations
    validation_success = runner.validate_latest_data()
    
    # Générer la documentation
    docs_success = runner.create_data_docs()
    
    if validation_success and docs_success:
        logger.info("✅ Toutes les opérations ont réussi!")
        return 0
    else:
        logger.error("❌ Certaines opérations ont échoué!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
