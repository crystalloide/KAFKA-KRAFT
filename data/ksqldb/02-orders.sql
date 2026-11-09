-- =============================================================================
--  ksqlDB - analyse en SQL d'un flux de commandes JSON (topic "orders-ksql",
--  alimenté à partir de data/orders.csv - même format que le topic "orders"
--  des TP Java/Spring)
-- =============================================================================
SET 'auto.offset.reset' = 'earliest';

-- Flux sur le topic existant (la clé du message = customerId)
CREATE STREAM orders_stream (
    customerId VARCHAR KEY,
    orderId    VARCHAR,
    product    VARCHAR,
    quantity   INT,
    unitPrice  DOUBLE,
    country    VARCHAR
) WITH (KAFKA_TOPIC='orders-ksql', VALUE_FORMAT='JSON');

-- Flux dérivé : commandes valides enrichies du montant (topic ORDERS_VALID créé par ksqlDB)
CREATE STREAM orders_valid WITH (KAFKA_TOPIC='orders_valid', PARTITIONS=3, REPLICAS=3) AS
  SELECT customerId, orderId, product, quantity, unitPrice, country,
         quantity * unitPrice AS amount
  FROM orders_stream
  WHERE quantity > 0
  EMIT CHANGES;

-- Table agrégée : chiffre d'affaires et nombre de commandes par pays
CREATE TABLE revenue_by_country WITH (KAFKA_TOPIC='revenue_by_country', PARTITIONS=3, REPLICAS=3) AS
  SELECT country,
         COUNT(*)            AS nb_orders,
         ROUND(SUM(amount), 2) AS revenue
  FROM orders_valid
  GROUP BY country
  EMIT CHANGES;

-- Fenêtre fixe ("tumbling") de 1 minute : nombre de commandes par client
CREATE TABLE orders_per_customer_1m WITH (KAFKA_TOPIC='orders_per_customer_1m', PARTITIONS=3, REPLICAS=3) AS
  SELECT customerId,
         COUNT(*) AS nb_orders
  FROM orders_valid
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY customerId
  EMIT CHANGES;
