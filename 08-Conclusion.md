# TP08 – Conclusion : bilan, futur de Kafka, ressources et quiz final

> **Chapitre 8 du programme**
> Durée : 30 min
> Reprend et met à jour : TP07 et TP12 (quiz Kafka / Kafka Connect et SMT) → quiz Kafka 4.4 KRaft

---

## Partie A – Bilan des deux jours

| Objectif pédagogique | TP | Ce que vous savez faire |
|---|---|---|
| 1. Composants et avantages de Kafka | TP01 | expliquer topic / partition / offset / broker / contrôleur KRaft ; installer un nœud |
| 2. Installer et configurer | TP01, TP02 | déployer un cluster KRaft 3 contrôleurs + 3 brokers avec les réglages de production |
| 3. API producer / consumer | TP03 | produire de façon fiable (acks, idempotence, transactions), consommer en groupe, gérer les commits et les erreurs (Java natif et Spring Kafka) |
| 4. Administrer | TP04 | configurer brokers et topics, gérer la rétention / compaction, quotas, redémarrage progressif, montée de version |
| 5. Superviser et optimiser | TP05 | lire les métriques clés, exploiter Prometheus / Grafana, mesurer et régler les performances |
| 6. Kafka Connect / Streams / écosystème | TP07a–e | intégrer des données (FileStream, Debezium, JDBC), traiter des flux, gérer les schémas, répliquer entre clusters |
| 7. Sécurité (synthèse) | TP06 | identifier les briques TLS, SASL, ACL et la configuration cliente associée |

---

## Partie B – Le futur de Kafka

**Ce qui a changé avec Kafka 4.x** (à retenir pour vos projets) :

- **KRaft uniquement** depuis 4.0 : ZooKeeper a disparu ; un cluster ZooKeeper doit d'abord être migré vers KRaft en 3.x (3.9, version « pont ») avant de passer en 4.x ;
- nouveau protocole de groupe **KIP-848** (`group.protocol=consumer`) : rééquilibrages incrémentaux gérés par le broker ; depuis 4.3, un message recommande de quitter le protocole `classic`, qui sera déprécié ;
- **Share groups** (KIP-932, « files de messages » dans Kafka) : consommation coopérative au message près, avec accusé de réception individuel ; la 4.4 ajoute les **dead-letter queues** de share groups (KIP-1191) ;
- **Kafka Streams** : protocole de groupe « streams » géré par le broker (KIP-1071, assignateurs côté broker en 4.4), **state stores transactionnels** en option (KIP-892, 4.4) ;
- **Tiered storage** (KIP-405, GA depuis 3.9) : segments anciens déportés vers un stockage objet ;
- **quorum KRaft dynamique** (KIP-853) et **ELR** (*Eligible Leader Replicas*, KIP-966) pour la disponibilité ;
- outillage : `kafka-cluster.sh api-versions` remplace `kafka-broker-api-versions.sh` (déprécié en 4.4).

**Ce qui arrive** :

- **Diskless topics** (KIP-1150, accepté en mars 2026) : topics stockés directement dans un stockage objet (S3…), sans réplication entre brokers — pour réduire les coûts réseau inter-zones dans le cloud. Seule la direction est validée : la conception détaillée (KIP-1163, KIP-1164) est encore en discussion et rien n'est livré dans une version ;
- **Kafka 5.0** : suppression de ce qui est déprécié en 4.x (module `streams-scala`, configuration `broker.id`, `kafka-broker-api-versions.sh`, anciennes métriques MirrorMaker, option `--property` des outils console…) ;
- rapprochement **streaming / lakehouse** : écriture des topics en tables **Apache Iceberg**, traitements Flink SQL ;
- moteurs « compatibles protocole Kafka » (Redpanda, WarpStream, AutoMQ, Aiven Inkless…) : le **protocole** Kafka est devenu un standard de fait.

**Conseils pour la suite** :

1. suivre les notes de mise à jour (`Upgrading` de la documentation) à chaque version mineure (≈ 3 par an) ;
2. monter de version de façon progressive : brokers → `metadata.version` / features → clients ;
3. migrer les consommateurs vers `group.protocol=consumer` dès que possible ;
4. automatiser (GitOps, Strimzi / opérateurs, topics déclarés dans Git) et superviser (TP05).

---

## Partie C – Documentation et ressources

| Ressource | Adresse |
|---|---|
| Documentation officielle Apache Kafka (4.4) | <https://kafka.apache.org/documentation/> |
| Propositions d'évolution (KIP) | <https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals> |
| Téléchargements et notes de version | <https://kafka.apache.org/downloads> |
| Spring for Apache Kafka (référence) | <https://docs.spring.io/spring-kafka/reference/> |
| Confluent Developer (cours gratuits, patterns) | <https://developer.confluent.io/> |
| Debezium | <https://debezium.io/documentation/> |
| Strimzi (Kafka sur Kubernetes) | <https://strimzi.io/documentation/> |
| Kafbat UI (console du TP) | <https://github.com/kafbat/kafka-ui> |
| Explorateur des paramètres et KIP | <https://kafka-options-explorer.conduktor.io/> |
| Liste de diffusion des utilisateurs | `users@kafka.apache.org` (<https://kafka.apache.org/contact>) |
| Dépôt de la formation | <https://github.com/crystalloide/KAFKA-KRAFT> |

---

## Partie D – Livres conseillés

| Livre | Auteurs | Éditeur | Pour qui |
|---|---|---|---|
| *Kafka: The Definitive Guide*, 2ᵉ éd. (2021) | G. Shapira, T. Palino, R. Sivaram, K. Petty | O'Reilly | la référence : architecture, fiabilité, exploitation |
| *Apache Kafka in Action: From Basics to Production* | A. Zelenin, A. Kropp | Manning | développeurs et exploitants, approche pratique récente |
| *Kafka Streams in Action*, 2ᵉ éd. (2024) | B. Bejeck | Manning | Kafka Streams et ksqlDB en profondeur |
| *Kafka Connect* (2023) | K. Stanley, M. Maison | O'Reilly | intégration de données, exploitation de Connect |
| *Kafka in Action* (2022) | D. Scott, V. Gamov, D. Klein | Manning | première approche, tous publics |
| *Kafka Troubleshooting in Production* (2023) | E. Eldor | Apress | incidents réels et résolution |
| *Designing Data-Intensive Applications*, 2ᵉ éd. (2026) | M. Kleppmann, C. Riccomini | O'Reilly | fondamentaux des systèmes de données distribués |

> Les livres antérieurs à 2022 décrivent encore ZooKeeper : les concepts restent valables, mais
> pas les commandes d'installation ni d'administration.

---

## Partie E – Quiz final

Répondez sans regarder la solution, puis dépliez la réponse.

**Question 1 – Comment lister les topics d'un cluster Kafka 4.4 ?**

- a) `kafka-topics.sh --describe --bootstrap-server broker1:9092,broker2:9092`
- b) `kafka-topics.sh --list --zookeeper zk1:2181`
- c) `kafka-topics.sh --list --bootstrap-server broker1:9092,broker2:9092`
- d) `kafka-topics.sh --describe --bootstrap-server broker1:9092 --topic __kafka_topic`

<details><summary>Réponse</summary>

**c**. `--describe` (a) affiche le détail des partitions ; `--zookeeper` (b) n'existe plus depuis Kafka 3.0 et ZooKeeper a disparu en 4.0.
</details>

**Question 2 – Créer un topic `Formation` avec 4 partitions et un facteur de réplication de 3 :**

- a) `kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --topic Formation`
- b) `kafka-create-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --replication 3 --topic Formation`
- c) `kafka-topics.sh --create --bootstrap-server localhost:9092,localhost:9094 --partitions 4 --replication-factor 3 --topic Formation`
- d) `kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --replication 3 --topic Formation`

<details><summary>Réponse</summary>

**c**. (a) utiliserait `num.partitions` par défaut ; `kafka-create-topics.sh` (b) n'existe pas ; l'option est `--replication-factor` (d).
</details>

**Question 3 – Sur un cluster de 3 brokers (ids 11, 12, 13), quel résultat est possible pour `--create --partitions 2 --replication-factor 2` ?**

- a) `Partition: 0 Leader: 11 Replicas: 11,12,13 Isr: 11,12,13`
- b) `Partition: 0 Leader: 11 Replicas: 11,12 Isr: 11,12` / `Partition: 1 Leader: 12 Replicas: 12,13 Isr: 12,13`
- c) `Partition: 1 Leader: 12 Replicas: 12,13` / `Partition: 2 Leader: 13 Replicas: 13,11`
- d) `Partition: 0 Leader: 11 Replicas: 11,11` / `Partition: 1 Leader: 12 Replicas: 12,12`

<details><summary>Réponse</summary>

**b**. 2 répliques par partition (pas 3 : a), partitions numérotées à partir de 0 (pas 1 et 2 : c), et deux répliques d'une même partition ne sont jamais sur le même broker (d). Le leader préféré est la **première** réplique de la liste.
</details>

**Question 4 – En mode KRaft, quel rôle gère les métadonnées du cluster ?**

- a) ZooKeeper
- b) le contrôleur actif, élu par le quorum Raft des contrôleurs
- c) le broker qui a le plus petit identifiant
- d) le Schema Registry

<details><summary>Réponse</summary>

**b**. Les métadonnées sont un journal (`__cluster_metadata`) répliqué par Raft entre les contrôleurs ; les brokers en suivent le contenu.
</details>

**Question 5 – Avec `replication.factor=3` et `min.insync.replicas=2`, un producer en `acks=all` :**

- a) continue d'écrire si 2 brokers sur 3 sont arrêtés
- b) continue d'écrire si 1 broker est arrêté, est refusé (`NotEnoughReplicas`) si 2 brokers sont arrêtés
- c) n'attend jamais les répliques
- d) perd des messages dès qu'un broker s'arrête

<details><summary>Réponse</summary>

**b**. Il faut au moins 2 répliques synchronisées pour acquitter une écriture `acks=all` : on tolère la perte d'un broker sans perte de données (TP02).
</details>

**Question 6 – Quelle configuration de producer garantit l'absence de doublons dus aux renvois ?**

- a) `acks=0`
- b) `enable.idempotence=true` (valeur par défaut avec `acks=all`)
- c) `retries=0`
- d) `linger.ms=0`

<details><summary>Réponse</summary>

**b**. L'idempotence (numéro de séquence par producer et par partition) supprime les doublons des renvois ; pour l'atomicité multi-partitions, il faut les **transactions**.
</details>

**Question 7 – Un topic a 6 partitions. Un groupe de 8 consommateurs s'y abonne :**

- a) les 8 consommateurs reçoivent tous les messages
- b) 6 consommateurs reçoivent chacun une partition, 2 restent inactifs
- c) les messages sont répartis au message près entre les 8
- d) le groupe est refusé

<details><summary>Réponse</summary>

**b**. Dans un consumer group, une partition n'est lue que par un seul membre. Le partage au message près (c) correspond aux **share groups** (KIP-932).
</details>

**Question 8 – Comment générer simplement des messages (clé et valeur) en ligne de commande ?**

- a) `kafka-console-consumer.sh ... --topic Formation`
- b) `kafka-shell.bat`
- c) `kafka-console-producer.sh --bootstrap-server localhost:9092 --topic Formation --property parse.key=true --property key.separator=:`
- d) un connecteur sink

<details><summary>Réponse</summary>

**c**. (Kafka Connect source, un producer Java ou `kafka-producer-perf-test.sh` conviennent aussi, mais pas un consumer ni un connecteur sink.)
</details>

**Question 9 – Lire tous les messages depuis le début du topic `Formation` :**

- a) `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Formation --from-beginning`
- b) `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic formation --from-beginning`
- c) `kafka-console-consumer.sh --zookeeper zk1:2181 --topic Formation --from-offset-earliest`
- d) `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Formation --all-offset`

<details><summary>Réponse</summary>

**a**. Les noms de topics sont sensibles à la casse (b lit un autre topic).
</details>

**Question 10 – Un topic compacté (`cleanup.policy=compact`) :**

- a) supprime tous les messages après `retention.ms`
- b) conserve au moins la dernière valeur de chaque clé ; un message de valeur nulle (tombstone) supprime la clé
- c) compresse les messages en gzip
- d) interdit les messages sans clé uniquement côté consumer

<details><summary>Réponse</summary>

**b** (TP04). La compaction ne s'applique pas au segment actif.
</details>

**Question 11 – Quelle métrique doit être à 0 en permanence sur un cluster sain ?**

- a) `MessagesInPerSec`
- b) `UnderReplicatedPartitions` / `OfflinePartitionsCount`
- c) `BytesOutPerSec`
- d) `RequestHandlerAvgIdlePercent`

<details><summary>Réponse</summary>

**b** (TP05). `RequestHandlerAvgIdlePercent` doit rester **élevé** (alerte sous 30 %).
</details>

**Question 12 – Que signifie SMT ?**

- a) Sample Message Transformation
- b) Simple Message Tropic
- c) Single Message Transformation
- d) Simple Monitoring Transmission

<details><summary>Réponse</summary>

**c**. Le nom officiel est *Single Message Transformation* (on lit souvent « Simple Message Transformation ») : une transformation appliquée message par message dans Kafka Connect.
</details>

**Question 13 – Les connecteurs qui écrivent les données de Kafka vers le monde extérieur sont :**

- a) connecteurs in-out
- b) connecteurs sink
- c) connecteurs source
- d) connecteurs kstream

<details><summary>Réponse</summary>

**b**. Les connecteurs **source** (c) lisent le monde extérieur pour alimenter Kafka (ex. : Debezium, TP07a).
</details>

**Question 14 – Quelles affirmations sont exactes sur les SMT ? (plusieurs réponses)**

- a) elles servent à effectuer des transformations simples, message par message
- b) elles ne sont utilisables que dans les connecteurs source
- c) elles ne sont utilisables qu'une fois par message
- d) elles sont chaînables (`transforms=a,b,c`) et conditionnables par des prédicats
- e) elles remplacent Kafka Streams pour les agrégations

<details><summary>Réponse</summary>

**a et d**. Elles s'appliquent aux connecteurs source **et** sink ; pour joindre ou agréger, il faut Kafka Streams / ksqlDB.
</details>

**Question 15 – Dans Kafka Streams, la différence entre `KStream` et `KTable` :**

- a) aucune
- b) `KStream` = flux d'événements ; `KTable` = dernier état par clé (changelog)
- c) `KTable` n'est utilisable qu'avec ksqlDB
- d) `KStream` stocke ses données dans ZooKeeper

<details><summary>Réponse</summary>

**b** (TP07b, TP07d).
</details>

**Question 16 – Avec le Schema Registry en compatibilité `BACKWARD`, quelle évolution est acceptée ?**

- a) ajouter un champ obligatoire sans valeur par défaut
- b) ajouter un champ avec une valeur par défaut
- c) changer le type d'un champ de `string` en `int`
- d) renommer un champ sans alias

<details><summary>Réponse</summary>

**b** (TP07c : `order-v2` accepté, `order-v3-incompatible` refusé avec une erreur 409).
</details>

**Question 17 – Avec MirrorMaker 2 et la politique par défaut, le topic `orders` du cluster `primary` devient sur le cluster `dr` :**

- a) `orders`
- b) `primary.orders`
- c) `dr.orders`
- d) `orders.mirror`

<details><summary>Réponse</summary>

**b** (TP07e). `IdentityReplicationPolicy` conserve le nom `orders`.
</details>

**Question 18 – Pour sécuriser un cluster, quelle association est correcte ?**

- a) TLS = autorisation, ACL = chiffrement
- b) TLS = chiffrement (et éventuellement authentification mTLS), SASL = authentification, ACL = autorisation
- c) SASL = chiffrement des disques
- d) ACL = pare-feu réseau

<details><summary>Réponse</summary>

**b** (TP06). Les ACL sont gérées en KRaft par `StandardAuthorizer`.
</details>

---

## Partie F – Nettoyage du poste (fin de formation)

```bash
cd ~/KAFKA-KRAFT
./scripts/stop-all.sh            # arrêt, données conservées
# ou remise à zéro complète :
./scripts/stop-all.sh --purge    # supprime conteneurs ET volumes
docker image prune -f
```

Nœud autonome du TP01 (si installé comme service) :

```bash
sudo systemctl disable --now kafka-standalone 2>/dev/null || true
```

Le dépôt `~/KAFKA-KRAFT`, les images Docker et l'installation `/opt/kafka` peuvent être conservés pour
rejouer les TP : `docker compose up -d` relance le cluster.

---

## Partie G – Planning indicatif des 2 jours (14 h)

| Jour | Créneau | Contenu |
|---|---|---|
| Avant | — | **TP00** : préparation du poste (WSL2, Docker, Java 21) — à faire en amont |
| J1 | 9 h 00 – 10 h 30 | Introduction, **TP01** (installation, premier nœud KRaft) |
| J1 | 10 h 45 – 12 h 30 | **TP02** (cluster 3 contrôleurs + 3 brokers, tolérance aux pannes) |
| J1 | 13 h 30 – 14 h 00 | **TP02** (suite : scalabilité, réassignation) |
| J1 | 14 h 00 – 17 h 30 | **TP03** (API producer / consumer, Java natif puis Spring Boot) |
| J2 | 9 h 00 – 10 h 15 | **TP04** (administration) |
| J2 | 10 h 30 – 11 h 45 | **TP05** (supervision et optimisation) |
| J2 | 11 h 45 – 12 h 15 | **TP06** (sécurité, synthèse) |
| J2 | 13 h 15 – 14 h 30 | **TP07a** (Kafka Connect, Debezium) |
| J2 | 14 h 30 – 15 h 45 | **TP07b** (Kafka Streams) |
| J2 | 16 h 00 – 17 h 00 | **TP07c / TP07d / TP07e** au choix du groupe (démonstration ou autonomie) |
| J2 | 17 h 00 – 17 h 30 | **TP08** (conclusion, quiz) |

> Les TP de l'écosystème (07c, 07d, 07e) sont autonomes : ceux qui ne sont pas traités en séance
> peuvent être réalisés après la formation avec le même environnement.

🎉 **Fin de la formation – merci !**
