-- Script de création des tables pour le projet ETL

-- Supprimer les tables si elles existent
DROP TABLE IF EXISTS target_table;
DROP TABLE IF EXISTS source_table;

-- Créer la table source
CREATE TABLE source_table (
    id SERIAL PRIMARY KEY,
    datenaissance DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Créer la table target
CREATE TABLE target_table (
    snapshot_date DATE NOT NULL,
    id INTEGER NOT NULL,
    datenaissance DATE,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date, id)
);
CREATE TABLE target_results_for_ge (
    snapshot_date DATE NOT NULL,
    id INTEGER NOT NULL,
    datenaissance DATE,
    age INTEGER,
    PRIMARY KEY (snapshot_date, id)
);

-- Créer des index pour améliorer les performances
CREATE INDEX idx_source_table_datenaissance ON source_table(datenaissance);
CREATE INDEX idx_target_table_snapshot_date ON target_table(snapshot_date);
CREATE INDEX idx_target_table_age ON target_table(age);

-- Ajouter des commentaires
COMMENT ON TABLE source_table IS 'Table source contenant les dates de naissance';
COMMENT ON TABLE target_table IS 'Table target avec les âges calculés par snapshot_date';
COMMENT ON COLUMN target_table.snapshot_date IS 'Date de référence pour le calcul de l''âge';
COMMENT ON COLUMN target_table.age IS 'Âge calculé basé sur datenaissance et snapshot_date';
