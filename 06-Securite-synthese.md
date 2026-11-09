# TP06 – Sécuriser un cluster Kafka (fiche de synthèse, sans manipulation)

> **Chapitre 6 du programme** – Objectif pédagogique n°7 : *à évoquer de façon synthétique*
> Durée : 30 min (présentation commentée par le formateur, **pas de travaux pratiques**)
> Reprend et met à jour : TP30* (ACL avec ZooKeeper → **StandardAuthorizer KRaft**), TP31 (sécurisation EC2)

> ⚠️ Les extraits de configuration ci-dessous sont des **exemples de référence** à adapter (mots de passe,
> noms d'hôtes, PKI de l'entreprise). Ils ne sont **pas** appliqués au cluster de formation, qui reste en
> `PLAINTEXT` pour simplifier les TP.

---

## 1. Les 4 couches de la sécurité Kafka

| Couche | Mécanisme | Question à laquelle elle répond |
|---|---|---|
| Réseau | filtrage (pare-feu, *security groups*, NetworkPolicy), listeners séparés | qui peut **joindre** les ports ? |
| Chiffrement | **TLS** (SSL) sur chaque listener | les données sont-elles **lisibles** sur le réseau ? |
| Authentification | **mTLS** ou **SASL** (SCRAM, OAUTHBEARER, GSSAPI/Kerberos, PLAIN) | **qui** est le client ? |
| Autorisation | **ACL** (StandardAuthorizer en KRaft), quotas | **que** peut-il faire ? |

Protocoles d'un listener (`listener.security.protocol.map`) : `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.
En production : **`SASL_SSL`** (ou `SSL` avec certificats clients) pour les clients, idem entre brokers et
vers les contrôleurs.

---

## 2. Filtrage réseau et pare-feu

| Flux | Port (exemple) | Source autorisée |
|---|---|---|
| Clients → brokers (`SASL_SSL`) | 9093 | réseaux applicatifs |
| Brokers ↔ brokers (réplication) | 9094 | brokers uniquement |
| Brokers / contrôleurs → contrôleurs (quorum) | 9095 | brokers et contrôleurs uniquement |
| Métriques (JMX Exporter) | 9404 | serveurs de supervision |
| Kafka Connect REST, Schema Registry, ksqlDB | 8083, 8081, 8088 | administrateurs / applications (à protéger aussi : TLS + authentification) |

Exemple `nftables` sur un broker (extrait) :

```text
table inet filter {
  chain input {
    type filter hook input priority 0; policy drop;
    ct state established,related accept
    tcp dport 9093 ip saddr 10.20.0.0/16 accept           # clients
    tcp dport 9094 ip saddr { 10.10.1.11, 10.10.1.12, 10.10.1.13 } accept   # inter-brokers
    tcp dport 9404 ip saddr 10.30.0.5 accept               # Prometheus
  }
}
```

Bonnes pratiques : un listener **par usage** (clients, réplication, contrôleurs), aucun port Kafka exposé
sur Internet, JMX distant désactivé (préférer l'agent exporter en lecture seule).

---

## 3. Chiffrement des communications (TLS)

Création d'un keystore (clé privée + certificat du broker) et d'un truststore (AC de confiance) :

```bash
keytool -genkeypair -alias broker-1 -keyalg RSA -keysize 3072 -validity 365 \
  -dname "CN=broker-1.kafka.example.com" -ext "SAN=DNS:broker-1.kafka.example.com" \
  -keystore broker-1.keystore.p12 -storetype PKCS12 -storepass "$KS_PASS"
# ... signature par l'AC de l'entreprise, puis import du certificat signé et de la chaîne ...
keytool -importcert -alias ca -file ca.crt -keystore truststore.p12 -storetype PKCS12 -storepass "$TS_PASS" -noprompt
```

Configuration d'un broker (extrait de `server.properties`) :

```properties
listeners=CLIENTS://:9093,REPLICATION://:9094
advertised.listeners=CLIENTS://broker-1.kafka.example.com:9093,REPLICATION://broker-1.internal:9094
listener.security.protocol.map=CLIENTS:SASL_SSL,REPLICATION:SSL,CONTROLLER:SSL
inter.broker.listener.name=REPLICATION
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/broker-1.keystore.p12
ssl.keystore.password=${file:/etc/kafka/secrets/passwords.properties:keystore}
ssl.key.password=${file:/etc/kafka/secrets/passwords.properties:key}
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=${file:/etc/kafka/secrets/passwords.properties:truststore}
# mTLS sur le listener de réplication : chaque broker présente son certificat
listener.name.replication.ssl.client.auth=required
ssl.endpoint.identification.algorithm=https
ssl.enabled.protocols=TLSv1.3,TLSv1.2
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

> Les certificats peuvent aussi être fournis au format **PEM** (`ssl.keystore.type=PEM`). Le *config provider*
> évite les mots de passe en clair dans les fichiers (autres providers : variables d'environnement, Vault...).
> Image Docker `apache/kafka` : variables `KAFKA_SSL_KEYSTORE_FILENAME`, `KAFKA_SSL_KEYSTORE_CREDENTIALS`,
> `KAFKA_SSL_KEY_CREDENTIALS` (fichiers montés dans `/etc/kafka/secrets`).

---

## 4. Authentification des applications clientes (SASL)

| Mécanisme | Principe | Usage |
|---|---|---|
| **SCRAM-SHA-512** | login/mot de passe salé, stocké dans les métadonnées KRaft | le plus simple à opérer |
| **OAUTHBEARER** (OIDC) | jeton JWT d'un fournisseur d'identité (Keycloak, Entra ID...) | intégration IAM d'entreprise |
| **GSSAPI** | Kerberos / Active Directory | environnements Kerberos existants |
| PLAIN | mot de passe en clair dans un fichier JAAS | uniquement sur TLS, tests |
| mTLS | certificat client (protocole `SSL`, `ssl.client.auth=required`) | machines / services |

Broker – activation de SCRAM sur le listener `CLIENTS` :

```properties
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.clients.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
```

Création des comptes (KRaft) :

```bash
# Au formatage du cluster (comptes nécessaires dès le démarrage, ex. compte inter-broker)
kafka-storage.sh format -t "$CLUSTER_ID" -c server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=admin-secret]'
# A chaud, pour une application
kafka-configs.sh --bootstrap-server broker-1:9093 --command-config admin.properties --alter \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=alice-secret]' --entity-type users --entity-name alice
kafka-configs.sh --bootstrap-server broker-1:9093 --command-config admin.properties --describe --entity-type users
```

Client Java (`client.properties` ou propriétés du producer/consumer) :

```properties
bootstrap.servers=broker-1.kafka.example.com:9093,broker-2.kafka.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="alice-secret";
ssl.truststore.location=/etc/app/truststore.p12
ssl.truststore.password=changeit
```

Spring Boot (`application.yml`) :

```yaml
spring:
  kafka:
    bootstrap-servers: broker-1.kafka.example.com:9093
    security:
      protocol: SASL_SSL
    ssl:
      trust-store-location: file:/etc/app/truststore.p12
      trust-store-password: ${TRUSTSTORE_PASSWORD}
    properties:
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
```

> Kafka 4.4 : un broker OAUTHBEARER configuré avec un JWKS **refuse de démarrer** si
> `sasl.oauthbearer.expected.audience` et `sasl.oauthbearer.expected.issuer` ne sont pas renseignés.

---

## 5. Permissions et autorisations sur les topics (ACL)

En KRaft, l'autorisateur est **`StandardAuthorizer`** : les ACL sont stockées dans le journal des
métadonnées (l'ancien `AclAuthorizer` basé sur ZooKeeper a disparu avec Kafka 4.0).

```properties
# brokers ET contrôleurs
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:broker
allow.everyone.if.no.acl.found=false
```

Principe : **tout est refusé** par défaut ; on accorde le **minimum** nécessaire.

| Rôle applicatif | Opérations nécessaires |
|---|---|
| Producer | `WRITE` + `DESCRIBE` sur le topic ; `IDEMPOTENT_WRITE` sur le cluster (implicite si WRITE sur le topic depuis 3.x) |
| Producer transactionnel | + `WRITE` / `DESCRIBE` sur le `TransactionalId` |
| Consumer | `READ` + `DESCRIBE` sur le topic, `READ` sur le **group** |
| Kafka Streams | `READ`/`WRITE` sur ses topics, `ALL` sur les topics internes préfixés par `application.id`, `READ` sur le groupe |
| Kafka Connect | topics internes du worker, groupe du worker, topics des connecteurs |

```bash
# Producteur "order-service" sur tous les topics préfixés "orders"
kafka-acls.sh --bootstrap-server broker-1:9093 --command-config admin.properties --add \
  --allow-principal User:order-service --producer --topic orders --resource-pattern-type prefixed
# Consommateur "billing" : lecture du topic orders avec le groupe billing
kafka-acls.sh --bootstrap-server broker-1:9093 --command-config admin.properties --add \
  --allow-principal User:billing --consumer --topic orders --group billing
# Interdiction explicite (un DENY l'emporte toujours sur un ALLOW)
kafka-acls.sh --bootstrap-server broker-1:9093 --command-config admin.properties --add \
  --deny-principal User:billing --operation Write --topic orders
# Consultation
kafka-acls.sh --bootstrap-server broker-1:9093 --command-config admin.properties --list --topic orders
```

```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)`:
 	(principal=User:billing, host=*, operation=READ, permissionType=ALLOW)
 	(principal=User:billing, host=*, operation=DESCRIBE, permissionType=ALLOW)
 	(principal=User:billing, host=*, operation=WRITE, permissionType=DENY)
```

Les ACL et les quotas par utilisateur (TP04) se combinent : un utilisateur authentifié a des droits **et**
des limites de débit.

---

## 6. Check-list de sécurisation

- [ ] Listeners séparés (clients / réplication / contrôleurs), **aucun PLAINTEXT** en production
- [ ] TLS 1.2+ partout, certificats émis par la PKI de l'entreprise, rotation planifiée (expiration surveillée)
- [ ] Authentification SASL (SCRAM ou OAUTHBEARER) ou mTLS pour **tous** les clients
- [ ] `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`, ACL minimales, `super.users` restreints
- [ ] Secrets via *config providers* (fichiers protégés, Vault), jamais en clair dans Git
- [ ] Sécuriser aussi Connect (REST), Schema Registry, ksqlDB, consoles web (SSO)
- [ ] Quotas par utilisateur ; audit des accès (logs `kafka.authorizer.logger`)
- [ ] Chiffrement des disques si exigé (Kafka ne chiffre pas les données au repos)

✅ **Suite : TP07 – Écosystème Kafka (Connect, Streams, Schema Registry, ksqlDB, MirrorMaker 2).**
