-- =============================================================================
--  Base "formation" : données sources du TP Kafka Connect (CDC Debezium)
--  Exécuté automatiquement au premier démarrage du conteneur postgres
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(200) NOT NULL UNIQUE,
    city        VARCHAR(100),
    loyalty     VARCHAR(20)  NOT NULL DEFAULT 'STANDARD'
);

-- Images "avant" complètes dans les événements UPDATE / DELETE
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;

INSERT INTO inventory.customers (first_name, last_name, email, city, loyalty) VALUES
  ('Alice',   'Martin',   'alice.martin@example.com',   'Paris',     'GOLD'),
  ('Bruno',   'Bernard',  'bruno.bernard@example.com',  'Lyon',      'STANDARD'),
  ('Chloé',   'Dubois',   'chloe.dubois@example.com',   'Marseille', 'SILVER'),
  ('David',   'Thomas',   'david.thomas@example.com',   'Toulouse',  'STANDARD'),
  ('Emma',    'Robert',   'emma.robert@example.com',    'Nantes',    'GOLD'),
  ('Farid',   'Richard',  'farid.richard@example.com',  'Lille',     'STANDARD'),
  ('Gaëlle',  'Petit',    'gaelle.petit@example.com',   'Rennes',    'SILVER'),
  ('Hugo',    'Durand',   'hugo.durand@example.com',    'Bordeaux',  'STANDARD');
