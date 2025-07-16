import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
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
        # Utiliser context_root_dir pour l'initialisation du contexte
        self.context = gx.get_context(context_root_dir=context_root_dir)
        
    def run_checkpoint(self, checkpoint_name: str = "target_results_checkpoint"):
        """Exécute un checkpoint Great Expectations défini dans un fichier YAML."""
        try:
            logger.info(f"Démarrage de la validation avec le checkpoint YAML: {checkpoint_name}")
            
            checkpoint = self.context.get_checkpoint(checkpoint_name)
            result = checkpoint.run()
            
            # Accès direct aux attributs de l'objet result
            if result.success:
                logger.info("✅ Toutes les validations du checkpoint YAML ont réussi!")
                self._log_validation_details(result)
                return True
            else:
                logger.error("❌ Certaines validations du checkpoint YAML ont échoué!")
                self._log_validation_details(result)
                return False
                
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du checkpoint YAML: {e}")
            return False

    def run_programmatic_checkpoint(self, snapshot_date: str):
        """
        Crée et exécute un checkpoint Great Expectations programmatiquement.
        """
        try:
            logger.info(f"Démarrage de la validation avec un checkpoint programmé pour la date: {snapshot_date}")

            # 1. Définir le BatchRequest programmatiquement
            batch_request = RuntimeBatchRequest(
                datasource_name="postgres_datasource",
                data_connector_name="default_runtime_data_connector",
                data_asset_name=settings.etl.target_results_for_ge,
                runtime_parameters={
                    "query": f"SELECT * FROM {settings.etl.target_results_for_ge} WHERE snapshot_date = '{snapshot_date}'"
                },
                batch_identifiers={
                    "default_identifier_name": f"{settings.etl.target_results_for_ge}_{snapshot_date.replace('-', '')}"
                }
            )

            # 2. Définir la liste des actions programmatiquement
            action_list = [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction", "site_names": ["local_site"]},
                },
            ]

            # 3. Créer le Checkpoint programmatiquement
            programmatic_checkpoint = Checkpoint(
                name=f"programmatic_target_validation_{snapshot_date.replace('-', '')}",
                data_context=self.context,
                batch_request=batch_request,
                expectation_suite_name="target_results_suite", # Utilise la suite d'expectations existante
                action_list=action_list,
                run_name_template=f"%Y%m%d-%H%M%S-programmatic-validation-{snapshot_date.replace('-', '')}"
            )

            # 4. Exécuter le checkpoint programmatiquement
            result = programmatic_checkpoint.run()

            if result.success:
                logger.info("✅ Toutes les validations du checkpoint programmé ont réussi!")
                self._log_validation_details(result)
                return True
            else:
                logger.error("❌ Certaines validations du checkpoint programmé ont échoué!")
                self._log_validation_details(result)
                return False

        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du checkpoint programmé: {e}")
            return False
    
    def _log_validation_details(self, result):
        """Log les détails des validations à partir d'un objet CheckpointResult."""
        # Itérer sur les objets ValidationResult (ou leurs représentations dict)
        for validation_result_wrapper in result.run_results.values():
            # Vérifier si l'objet est un dictionnaire et contient la clé "validation_result"
            # C'est le cas pour certaines versions/configurations de Great Expectations
            if isinstance(validation_result_wrapper, dict) and "validation_result" in validation_result_wrapper:
                vr = validation_result_wrapper["validation_result"]
            else:
                # Sinon, supposer que validation_result_wrapper est l'objet de résultat de validation lui-même
                vr = validation_result_wrapper

            # Accéder aux propriétés en utilisant .get() pour la robustesse
            suite_name = vr.get("meta", {}).get("expectation_suite_name", "Unknown")
            success_count = vr.get("statistics", {}).get("successful_expectations", 0)
            total_count = vr.get("statistics", {}).get("evaluated_expectations", 0)
            
            logger.info(f"📘 Suite: {suite_name}")
            logger.info(f"✅ Expectations réussies: {success_count}/{total_count}")
            
            # Vérifier le succès global de ce résultat de validation spécifique
            if not vr.get("success", True):
                for result_item in vr.get("results", []):
                    if not result_item.get("success", True):
                        expectation_type = result_item.get("expectation_config", {}).get("expectation_type", "inconnue")
                        logger.error(f"❌ Échec: {expectation_type}")
                        unexpected_values = result_item.get("result", {}).get("partial_unexpected_list", [])
                        if unexpected_values:
                            logger.error(f"    ⚠️ Valeurs inattendues (extrait): {unexpected_values[:5]}")
    
    def create_data_docs(self):
        """Génère la documentation des données."""
        try:
            self.context.build_data_docs()
            logger.info("📊 Documentation des données générée avec succès")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la génération de la documentation: {e}")
            return False
    
    def validate_latest_data(self):
        """Valide les données les plus récentes en utilisant le checkpoint YAML."""
        return self.run_checkpoint("target_results_checkpoint")

def main():
    """Fonction principale pour exécuter les validations"""
    runner = GreatExpectationsRunner()
    
    logger.info("🚀 Démarrage des validations Great Expectations")
    
    # Exécuter les validations en utilisant le checkpoint programmé
    # Vous pouvez remplacer la date par une variable dynamique si nécessaire
    current_snapshot_date = datetime.now().strftime('%Y-%m-%d')
    validation_success = runner.run_programmatic_checkpoint(current_snapshot_date)
    
    # Générer la documentation (cela inclura les résultats de toutes les exécutions)
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
