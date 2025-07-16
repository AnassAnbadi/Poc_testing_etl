# Projet ETL avec Tests Pytest vs Great Expectations

Ce projet implémente un pipeline ETL complet avec PostgreSQL, comparant les approches de test avec **pytest** et **Great Expectations**.

## 🏗️ Architecture

\`\`\`
Poc_testing_etl/
├── etl/                        # Code ETL métier
│   ├── extract.py             # Extraction des données
│   ├── transform.py           # Transformation et calcul d'âge
│   ├── load.py               # Chargement vers target
│   └── utils.py              # Utilitaires communs
├── config/                    # Configuration
│   └── settings.py           # Paramètres DB et ETL
├── great_expectations/        # Configuration Great Expectations
│   ├── checkpoints/          # Points de contrôle
│   ├── expectations/         # Suites de validation
│   └── great_expectations.yml # Config principale
├── ge_runner/                # Runner Great Expectations
│   └── run_validation.py     # Exécution des validations
├── tests/                    # Tests pytest
│   ├── conftest.py          # Configuration des tests
│   └── test_etl_pipeline.py # Tests ETL complets
├── scripts/                  # Scripts SQL
│   ├── 01_create_tables.sql # Création des tables
│   └── 02_insert_sample_data.sql # Données d'exemple
├── main.py                   # Orchestration principale
├── .env                      # Variables d'environnement
└── requirements.txt          # Dépendances Python
\`\`\`

## 🎯 Objectif

Comparer **pytest** et **Great Expectations** pour la validation de données dans un contexte ETL :

### Source Table
- **Colonnes** : \`id\`, \`datenaissance\`

### Target Table  
- **Colonnes** : \`snapshot_date\`, \`id\`, \`datenaissance\`, \`age\`
- **Logique** : Calculer l'âge basé sur \`datenaissance\` et \`snapshot_date\`

## 🚀 Installation

### 1. Prérequis
- Python 3.8+
- PostgreSQL 12+
- pip

### 2. Installation des dépendances
\`\`\`bash
pip install -r requirements.txt
\`\`\`

### 3. Configuration de la base de données
\`\`\`bash
# Créer la base de données
createdb etl_db

# Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos paramètres PostgreSQL
\`\`\`

### 4. Initialisation
\`\`\`bash
# Configurer la base de données et insérer des données d'exemple
python main.py --setup-only
\`\`\`

## 🔧 Utilisation

### Pipeline ETL Complet
\`\`\`bash
# Exécuter le pipeline complet (ETL + validations)
python main.py

# Avec une date de snapshot spécifique
python main.py --snapshot-date 2024-01-15
\`\`\`

### Exécution Sélective
\`\`\`bash
# ETL seulement
python main.py --etl-only

# Validations Great Expectations seulement
python main.py --validate-only

# Configuration de la base seulement
python main.py --setup-only
\`\`\`

## 🧪 Tests

### Tests Pytest
\`\`\`bash
# Exécuter tous les tests
pytest tests/ -v

# Tests avec couverture
pytest tests/ --cov=etl --cov-report=html

# Tests spécifiques
pytest tests/test_etl_pipeline.py::TestETLPipeline::test_calculate_age_function -v
\`\`\`

### Validations Great Expectations
\`\`\`bash
# Exécuter les validations GE
python ge_runner/run_validation.py

# Ou via le pipeline principal
python main.py --validate-only
\`\`\`

## 📊 Comparaison Pytest vs Great Expectations

### Pytest ✅
**Avantages :**
- Contrôle granulaire des tests
- Intégration native avec le code Python
- Flexibilité maximale pour les tests complexes
- Excellent pour les tests unitaires et d'intégration
- Debugging facile

**Inconvénients :**
- Nécessite plus de code pour les validations de données
- Pas de documentation automatique des règles métier
- Moins adapté pour les non-développeurs

### Great Expectations 📈
**Avantages :**
- Spécialisé pour la validation de données
- Documentation automatique des attentes
- Interface web pour visualiser les résultats
- Déclaratif et lisible par les métiers
- Intégration native avec de nombreuses sources de données

**Inconvénients :**
- Courbe d'apprentissage plus élevée
- Moins flexible pour les validations complexes
- Configuration plus lourde
- Dépendance externe

## 📈 Métriques de Qualité des Données

### Tests Pytest Implémentés
- ✅ Calcul correct de l'âge
- ✅ Gestion des cas limites (anniversaires, dates futures)
- ✅ Intégrité des données (pas de doublons)
- ✅ Complétude des données
- ✅ Cohérence des transformations
- ✅ Pipeline ETL end-to-end

### Validations Great Expectations
- ✅ Nombre de lignes dans les limites attendues
- ✅ Absence de valeurs nulles
- ✅ Types de données corrects
- ✅ Âges dans la plage valide (0-150)
- ✅ Unicité des clés composites
- ✅ Format des dates

## 🔍 Monitoring et Observabilité

### Logs
- Logs structurés avec niveaux appropriés
- Traçabilité complète du pipeline
- Métriques de performance

### Documentation des Données
\`\`\`bash
# Générer la documentation Great Expectations
python ge_runner/run_validation.py
# Ouvrir great_expectations/uncommitted/data_docs/local_site/index.html
\`\`\`

## 🛠️ Développement

### Structure du Code
- **Séparation des responsabilités** : Extract, Transform, Load
- **Configuration centralisée** : settings.py
- **Gestion des erreurs** : try/catch avec logs appropriés
- **Tests complets** : unitaires, intégration, end-to-end

### Bonnes Pratiques
- Utilisation de context managers pour les connexions DB
- Validation des données à chaque étape
- Logging structuré
- Configuration par variables d'environnement
- Tests automatisés

## 📝 Conclusion

Ce projet démontre que :

1. **Pytest** excelle pour les tests de logique métier et les validations complexes
2. **Great Expectations** est idéal pour les validations de données standardisées et la documentation
3. **L'approche hybride** (les deux ensemble) offre la meilleure couverture

### Recommandations
- Utilisez **pytest** pour les tests unitaires et la logique métier
- Utilisez **Great Expectations** pour les validations de données et la documentation
- Implémentez les deux pour une couverture complète de la qualité des données
\`\`\`

## 🤝 Contribution

1. Fork le projet
2. Créer une branche feature
3. Commit les changements
4. Push vers la branche
5. Ouvrir une Pull Request

