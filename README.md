# 🚀 ETL Testing PoC - Age Calculation Pipeline

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-12%2B-blue.svg)](https://postgresql.org)
[![pytest](https://img.shields.io/badge/testing-pytest-green.svg)](https://pytest.org)
[![Great Expectations](https://img.shields.io/badge/validation-great--expectations-orange.svg)](https://greatexpectations.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Proof of Concept (PoC)** démontrant les meilleures pratiques de test et validation dans un pipeline ETL moderne avec **pytest** et **Great Expectations**.

## 📋 Table des matières

- [🎯 Objectif du projet](#-objectif-du-projet)
- [🏗️ Architecture](#️-architecture)
- [⚡ Quick Start](#-quick-start)
- [📊 Modèle de données](#-modèle-de-données)
- [🔄 Pipeline ETL](#-pipeline-etl)
- [🧪 Stratégie de test](#-stratégie-de-test)
- [📈 Validation des données](#-validation-des-données)
- [🚀 Utilisation](#-utilisation)
- [🔧 Configuration](#-configuration)
- [📚 Documentation technique](#-documentation-technique)
- [🤝 Contribution](#-contribution)

## 🎯 Objectif du projet

Ce PoC illustre comment implémenter un pipeline ETL robuste avec une stratégie de test complète combinant :

- **Tests unitaires et d'intégration** avec pytest
- **Validation de qualité des données** avec Great Expectations
- **Calcul métier** : mise à jour automatique de l'âge basé sur la date de naissance
- **Architecture modulaire** et maintenable

### 🎪 Cas d'usage

**Problématique** : Mettre à jour les colonnes `datenaissance` et `age` dans une table target basé sur une `snapshot_date` et valider la qualité des données.

**Solution** : Pipeline ETL avec double validation (pytest + Great Expectations)

## 🏗️ Architecture

\`\`\`
etl_project/
├── 📁 etl/                        # 🔧 Code ETL métier
│   ├── extract.py                 # 📥 Extraction des données
│   ├── transform.py               # 🔄 Transformation et calcul d'âge
│   ├── load.py                    # 📤 Chargement des données
│   └── utils.py                   # 🛠️ Utilitaires et connexion DB
├── 📁 config/                     # ⚙️ Configuration
│   └── settings.py                # 🔧 Paramètres DB et ETL
├── 📁 great_expectations/         # 📊 Validation Great Expectations
│   ├── checkpoints/              # 📌 Points de contrôle
│   │   └── target_results_checkpoint.yml
│   ├── expectations/             # 📋 Suites d'attentes
│   │   └── target_results_suite.json
│   ├── validations/              # ✅ Résultats des validations
│   └── great_expectations.yml    # 🔧 Configuration principale
├── 📁 ge_runner/                  # 🏃 Runner Great Expectations
│   └── run_validation.py         # ▶️ Exécution des validations
├── 📁 tests/                      # 🧪 Tests pytest
│   ├── test_etl.py               # 🔬 Tests unitaires ETL
│   └── conftest.py               # ⚙️ Configuration pytest
├── 📁 scripts/                    # 📜 Scripts SQL
│   └── create_tables.sql         # 🗃️ Création des tables
├── 📁 logs/                       # 📝 Fichiers de logs
├── main.py                       # 🎯 Orchestration pipeline
├── .env                          # 🔐 Variables d'environnement
├── requirements.txt              # 📦 Dépendances Python
└── README.md                     # 📖 Documentation
\`\`\`

### 🔧 Stack technique

| Composant | Technologie | Version | Rôle |
|-----------|-------------|---------|------|
| **Base de données** | PostgreSQL | 12+ | Stockage des données |
| **Langage** | Python | 3.8+ | Développement ETL |
| **ORM/DB** | SQLAlchemy + psycopg2 | 2.0+ | Connexion base de données |
| **Data Processing** | Pandas | 2.1+ | Manipulation des données |
| **Tests unitaires** | pytest | 7.4+ | Tests et couverture |
| **Validation données** | Great Expectations | 0.18+ | Qualité des données |
| **Configuration** | Pydantic + python-dotenv | 2.5+ | Gestion config |
| **Logging** | Loguru | 0.7+ | Logs structurés |

## ⚡ Quick Start

### 1️⃣ Prérequis

\`\`\`bash
# Vérifier les versions
python --version  # >= 3.8
psql --version    # >= 12
\`\`\`

### 2️⃣ Installation

\`\`\`bash
# Cloner le repository
git clone https://github.com/AnassAnbadi/Poc_testing_etl.git
cd Poc_testing_etl

# Créer un environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\\Scripts\\activate     # Windows

# Installer les dépendances
pip install -r requirements.txt
\`\`\`

### 3️⃣ Configuration base de données

\`\`\`bash
# Créer la base de données
createdb etl_db

# Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos paramètres
\`\`\`

### 4️⃣ Lancement rapide

\`\`\`bash
# Setup complet + exécution
python main.py

# Ou étape par étape
python main.py --setup-only      # Setup uniquement
python main.py --snapshot-date 2024-01-01  # Date spécifique
\`\`\`

## 📊 Modèle de données

### 🗃️ Table source (\`source_table\`)

| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| \`id\` | INTEGER | PRIMARY KEY | Identifiant unique |
| \`datenaissance\` | DATE | NOT NULL | Date de naissance |
| \`created_at\` | TIMESTAMP | DEFAULT NOW() | Date de création |

### 🎯 Table target (\`target_table\`)

| Colonne | Type | Contrainte | Description |
|---------|------|------------|-------------|
| \`snapshot_date\` | DATE | PRIMARY KEY | Date de snapshot |
| \`id\` | INTEGER | PRIMARY KEY | Référence vers source |
| \`datenaissance\` | DATE | - | **À remplir par ETL** |
| \`age\` | INTEGER | - | **À calculer par ETL** |
| \`updated_at\` | TIMESTAMP | DEFAULT NOW() | Date de mise à jour |

### 🔄 Logique métier

\`\`\`python
# Calcul d'âge
age = snapshot_date.year - birth_date.year
if (snapshot_date.month, snapshot_date.day) < (birth_date.month, birth_date.day):
    age -= 1
\`\`\`

## 🔄 Pipeline ETL

### 📥 Extract (Extraction)

\`\`\`python
# Extraction des données source
source_df = extractor.extract_source_data()

# Extraction des données target pour une snapshot
target_df = extractor.extract_target_data(snapshot_date)
\`\`\`

### 🔄 Transform (Transformation)

\`\`\`python
# Transformation avec calcul d'âge
transformed_df = transformer.transform_data(
    source_df, target_df, snapshot_date
)
\`\`\`

**Étapes de transformation :**
1. ✅ Merge des données source/target sur l'ID
2. ✅ Mise à jour des dates de naissance manquantes
3. ✅ Calcul automatique de l'âge
4. ✅ Validation des données transformées

### 📤 Load (Chargement)

\`\`\`python
# Chargement avec gestion des doublons
success = loader.load_to_target(transformed_df, snapshot_date)
\`\`\`

**Stratégie de chargement :**
- 🗑️ Suppression des données existantes pour la snapshot
- 📦 Insertion par batch pour les performances
- 🔄 Transaction atomique

## 🧪 Stratégie de test

### 🔬 Tests pytest

#### Tests unitaires

\`\`\`bash
# Exécuter tous les tests
pytest tests/ -v

# Avec couverture de code
pytest tests/ -v --cov=etl --cov-report=html

# Tests spécifiques
pytest tests/test_etl.py::TestETLUtils::test_calculate_age_normal_case -v
\`\`\`

#### Couverture des tests

| Module | Fonctionnalité | Tests |
|--------|----------------|-------|
| **utils.py** | Calcul d'âge | ✅ Cas normaux, limites, erreurs |
| **extract.py** | Extraction données | ✅ Mocking DB, gestion erreurs |
| **transform.py** | Transformation | ✅ Logique métier, validation |
| **load.py** | Chargement | ✅ Insertion, gestion conflits |
| **Pipeline** | Intégration | ✅ End-to-end, rollback |

#### Exemples de tests

\`\`\`python
def test_calculate_age_normal_case():
    """Test calcul d'âge standard"""
    birth_date = date(1990, 5, 15)
    snapshot_date = date(2024, 1, 1)
    age = calculate_age(birth_date, snapshot_date)
    assert age == 33

def test_transform_data_success():
    """Test transformation complète"""
    result_df = transformer.transform_data(source_df, target_df, '2024-01-01')
    assert result_df['age'].notna().all()
    assert result_df['datenaissance'].notna().all()
\`\`\`

## 📈 Validation des données

### 🎯 Great Expectations

#### Configuration

\`\`\`yaml
# great_expectations/great_expectations.yml
datasources:
  postgres_datasource:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: postgresql://user:pass@localhost:5432/etl_db
\`\`\`

#### Expectations implémentées

| Catégorie | Expectation | Description |
|-----------|-------------|-------------|
| **Structure** | \`expect_column_to_exist\` | Colonnes requises présentes |
| **Complétude** | \`expect_column_values_to_not_be_null\` | Pas de valeurs nulles |
| **Type** | \`expect_column_values_to_be_of_type\` | Types de données corrects |
| **Domaine** | \`expect_column_values_to_be_between\` | Âges réalistes (0-150) |
| **Format** | \`expect_column_values_to_match_strftime_format\` | Format dates |
| **Cohérence** | \`expect_column_pair_values_A_to_be_greater_than_B\` | snapshot_date > datenaissance |

#### Exécution des validations

\`\`\`bash
# Validation complète
python ge_runner/run_validation.py

# Validation d'une snapshot spécifique
python -c "
from ge_runner.run_validation import GreatExpectationsRunner
runner = GreatExpectationsRunner()
runner.validate_target_table('2024-01-01')
"
\`\`\`

#### Rapport de validation

Après validation, consulter le rapport HTML :
\`\`\`
great_expectations/uncommitted/data_docs/local_site/index.html
\`\`\`

## 🚀 Utilisation

### 🎯 Commandes principales

\`\`\`bash
# 🔧 Setup initial (tables + données d'exemple)
python main.py --setup-only

# 🚀 Pipeline complet (toutes les snapshots)
python main.py

# 📅 Traitement d'une snapshot spécifique
python main.py --snapshot-date 2024-01-01

# 🔍 Validation uniquement
python main.py --validate-only

# ⚡ Sans validation Great Expectations
python main.py --skip-validation
\`\`\`

### 📊 Monitoring

#### Logs

\`\`\`bash
# Logs en temps réel
tail -f logs/etl_pipeline_$(date +%Y-%m-%d).log

# Logs avec niveau DEBUG
export LOG_LEVEL=DEBUG
python main.py
\`\`\`

#### Métriques

Le pipeline génère automatiquement :
- 📈 Nombre d'enregistrements traités
- ⏱️ Temps d'exécution par étape
- ✅ Taux de succès des validations
- 🚨 Alertes en cas d'erreur

### 🔄 Intégration continue

\`\`\`bash
# Script de test complet
#!/bin/bash
set -e

echo "🧪 Exécution des tests..."
pytest tests/ -v --cov=etl --cov-report=term-missing

echo "🔍 Validation des données..."
python ge_runner/run_validation.py

echo "🚀 Test du pipeline..."
python main.py --snapshot-date 2024-01-01

echo "✅ Tous les tests passés!"
\`\`\`

## 🔧 Configuration

### 📁 Variables d'environnement (\`.env\`)

\`\`\`env
# 🗃️ Configuration base de données
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=postgres
DB_PASSWORD=your_secure_password

# ⚙️ Configuration ETL
BATCH_SIZE=1000
MAX_RETRIES=3
LOG_LEVEL=INFO

# 🔍 Configuration Great Expectations
GE_CONTEXT_ROOT_DIR=great_expectations
\`\`\`

### ⚙️ Paramètres avancés

\`\`\`python
# config/settings.py
class ETLConfig(BaseSettings):
    batch_size: int = 1000          # Taille des lots
    max_retries: int = 3            # Tentatives en cas d'erreur
    log_level: str = "INFO"         # Niveau de logging
    timeout_seconds: int = 300      # Timeout des requêtes
\`\`\`

## 📚 Documentation technique

### 🏗️ Patterns utilisés

- **🏭 Factory Pattern** : Création des connexions DB
- **🔧 Builder Pattern** : Construction des requêtes SQL
- **📋 Strategy Pattern** : Différentes stratégies de validation
- **🎯 Dependency Injection** : Configuration modulaire

### 🔍 Bonnes pratiques

#### ✅ Code Quality

- **Type hints** partout
- **Docstrings** pour toutes les fonctions
- **Logging structuré** avec contexte
- **Gestion d'erreurs** robuste
- **Tests** avec couverture > 90%

#### 🚀 Performance

- **Traitement par batch** pour les gros volumes
- **Connexions poolées** pour la DB
- **Requêtes optimisées** avec index
- **Monitoring** des performances

#### 🔒 Sécurité

- **Variables d'environnement** pour les secrets
- **Validation des entrées** utilisateur
- **Transactions atomiques**
- **Logs sans données sensibles**

### 🐛 Dépannage

#### Erreurs communes

\`\`\`bash
# ❌ Erreur de connexion DB
ERROR: could not connect to server
# ✅ Solution
export DB_HOST=localhost
export DB_PASSWORD=correct_password

# ❌ Tests pytest échouent
ImportError: No module named 'etl'
# ✅ Solution
export PYTHONPATH=\${PWD}:\${PYTHONPATH}

# ❌ Great Expectations erreur
DataContextError: Unable to load config
# ✅ Solution
great_expectations init --force
\`\`\`

#### Mode debug

\`\`\`bash
# 🔍 Debug complet
export LOG_LEVEL=DEBUG
python -m pdb main.py

# 🧪 Tests avec détails
pytest tests/ -v -s --tb=long --pdb
\`\`\`

## 🤝 Contribution

### 🚀 Comment contribuer

1. **Fork** le projet
2. **Créer** une branche feature (\`git checkout -b feature/amazing-feature\`)
3. **Commit** vos changements (\`git commit -m 'Add amazing feature'\`)
4. **Push** vers la branche (\`git push origin feature/amazing-feature\`)
5. **Ouvrir** une Pull Request

### 📋 Checklist avant PR

- [ ] ✅ Tests passent (\`pytest tests/ -v\`)
- [ ] ✅ Couverture > 90% (\`pytest --cov=etl\`)
- [ ] ✅ Great Expectations OK (\`python ge_runner/run_validation.py\`)
- [ ] ✅ Code formaté (\`black etl/ tests/\`)
- [ ] ✅ Linting OK (\`flake8 etl/ tests/\`)
- [ ] ✅ Documentation mise à jour

### 🎯 Roadmap

- [ ] 🐳 **Docker** : Containerisation complète
- [ ] 🔄 **CI/CD** : GitHub Actions
- [ ] 📊 **Monitoring** : Grafana + Prometheus
- [ ] 🚀 **Airflow** : Orchestration avancée
- [ ] 📈 **Data Lineage** : Traçabilité des données
- [ ] 🔍 **Data Profiling** : Analyse automatique

---

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 👥 Auteurs

- **Anass Anbadi** - *Développeur principal* - [@AnassAnbadi](https://github.com/AnassAnbadi)

## 🙏 Remerciements

- **Great Expectations** pour la validation des données
- **pytest** pour le framework de test
- **SQLAlchemy** pour l'ORM
- **Pandas** pour la manipulation des données

---

<div align="center">

**⭐ Si ce projet vous aide, n'hésitez pas à lui donner une étoile ! ⭐**

[🐛 Signaler un bug](https://github.com/AnassAnbadi/Poc_testing_etl/issues) • 
[💡 Demander une fonctionnalité](https://github.com/AnassAnbadi/Poc_testing_etl/issues) • 
[📖 Documentation](https://github.com/AnassAnbadi/Poc_testing_etl/wiki)

</div>
\`\`\`
