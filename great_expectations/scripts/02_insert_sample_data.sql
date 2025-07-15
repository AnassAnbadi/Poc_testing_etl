-- Script d'insertion de données d'exemple

-- Insérer des données d'exemple dans la table source
INSERT INTO source_table (datenaissance) VALUES
    ('1990-05-15'),
    ('1985-12-03'),
    ('2000-08-22'),
    ('1975-03-10'),
    ('1995-11-28'),
    ('1988-07-14'),
    ('1992-02-29'),
    ('1980-09-05'),
    ('1998-12-25'),
    ('1987-04-18'),
    ('1993-10-30'),
    ('1982-06-12'),
    ('1996-01-08'),
    ('1979-11-22'),
    ('2001-03-17')
ON CONFLICT DO NOTHING;

-- Vérifier les données insérées
SELECT 
    COUNT(*) as total_records,
    MIN(datenaissance) as oldest_birth_date,
    MAX(datenaissance) as youngest_birth_date
FROM source_table;
