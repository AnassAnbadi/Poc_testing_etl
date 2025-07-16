import great_expectations as gx
import os
import sys

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings

def add_new_expectation():
    """
    Ajoute une nouvelle expectation à la suite existante 'target_results_suite'.
    """
    try:
        # Obtenir le chemin absolu du répertoire great_expectations
        context_root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "great_expectations"))
        project_config_path = os.path.join(context_root_dir, "great_expectations.yml")
        
        # 1. Charger le DataContext
        context = gx.get_context(project_config_path=project_config_path)
        print(f"DataContext chargé depuis: {project_config_path}")

        # 2. Obtenir un Validator pour la table target_table
        # Nous utilisons un BatchRequest pour spécifier la table et la requête
        batch_request = {
            "datasource_name": "postgres_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": settings.etl.target_table,
            "runtime_parameters": {
                "query": f"SELECT * FROM {settings.etl.target_table} WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {settings.etl.target_table})"
            },
            "batch_identifiers": {
                "default_identifier_name": f"{settings.etl.target_table}_latest"
            }
        }
        
        # Obtenir le Validator pour la suite d'expectations existante
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="target_results_suite"
        )
        print(f"Validator obtenu pour la suite: {validator.expectation_suite_name}")

        # 3. Ajouter la nouvelle expectation
        # expect_column_values_to_be_dateutil_parseable est plus approprié pour les colonnes DATE
        validator.expect_column_values_to_be_dateutil_parseable(
            column="datenaissance",
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "La colonne datenaissance doit être parsable comme une date valide."
                }
            }
        )
        print("Nouvelle expectation 'expect_column_values_to_be_dateutil_parseable' ajoutée pour 'datenaissance'.")

        # 4. Sauvegarder la suite d'expectations mise à jour
        validator.save_expectation_suite(discard_failed_expectations=False)
        print(f"Suite d'expectations '{validator.expectation_suite_name}' sauvegardée avec succès.")

        # Optionnel: Reconstruire la documentation pour voir la nouvelle expectation
        context.build_data_docs()
        print("Documentation des données reconstruite.")

    except Exception as e:
        print(f"Une erreur est survenue: {e}")
        sys.exit(1)

if __name__ == "__main__":
    add_new_expectation()
