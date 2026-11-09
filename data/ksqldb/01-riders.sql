-- =============================================================================
--  ksqlDB - exemple "riders" (repris du TP21) : flux, table matérialisée,
--  requêtes push et pull. A exécuter dans la CLI ksql.
-- =============================================================================
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)
  WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=3);

CREATE TABLE currentLocation AS
  SELECT profileId,
         LATEST_BY_OFFSET(latitude)  AS la,
         LATEST_BY_OFFSET(longitude) AS lo
  FROM riderLocations
  GROUP BY profileId
  EMIT CHANGES;

CREATE TABLE ridersNearMountainView AS
  SELECT ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1) AS distanceInMiles,
         COLLECT_LIST(profileId) AS riders,
         COUNT(*) AS count
  FROM currentLocation
  GROUP BY ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1);

INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);
