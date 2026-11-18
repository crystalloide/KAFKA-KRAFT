## TP04 – Administrer un cluster Kafka

> **Chapitre 4 du programme** – Objectif pédagogique n°4 (mise en oeuvre et administration, sans déploiement de cluster)

> Durée : 1 h 45

> Pré-requis : cluster du TP02 démarré, outils CLI (TP01), `source ~/KAFKA-KRAFT/scripts/kafka-env.sh`


---

## Objectifs

- Configurer et administrer les **brokers** (configuration statique / dynamique)
- Configurer les **topics** : rétention, purge, **compaction**, grands messages
- Mettre en place des **quotas**
- Appliquer les procédures de **redémarrage à chaud** et de **montée de version**
- **Automatiser** les opérations (scripts idempotents, API Admin, "topics as code")
- Comprendre la **configuration réseau** de Kafka dans Docker
- Utiliser les **consoles d'administration web** (Kafka UI, AKHQ)

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
cd ~/KAFKA-KRAFT
```

---

## Partie A – Configuration et administration des brokers

### A.1 Sources de configuration

Chaque paramètre a une **source** (de la plus prioritaire à la moins prioritaire) :

| Source | Où | Modification |
|---|---|---|
| `DYNAMIC_BROKER_CONFIG` | métadonnées KRaft, pour **un** broker | `kafka-configs.sh --entity-name 11` |
| `DYNAMIC_DEFAULT_BROKER_CONFIG` | métadonnées KRaft, **tous** les brokers | `kafka-configs.sh --entity-default` |
| `STATIC_BROKER_CONFIG` | fichier `server.properties` (ici généré depuis `KAFKA_*`) | redémarrage du broker |
| `DEFAULT_CONFIG` | valeur par défaut du code | — |

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 11 --describe --all \
  | grep -E "num.io.threads|min.insync.replicas|log.retention.hours|auto.create.topics.enable|message.max.bytes"
```

```text
  auto.create.topics.enable=false sensitive=false synonyms={STATIC_BROKER_CONFIG:auto.create.topics.enable=false, DEFAULT_CONFIG:auto.create.topics.enable=true}
  log.retention.hours=168 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=168, DEFAULT_CONFIG:log.retention.hours=168}
  message.max.bytes=1048588 sensitive=false synonyms={DEFAULT_CONFIG:message.max.bytes=1048588}
  min.insync.replicas=2 sensitive=false synonyms={STATIC_BROKER_CONFIG:min.insync.replicas=2, DEFAULT_CONFIG:min.insync.replicas=1}
  num.io.threads=8 sensitive=false synonyms={STATIC_BROKER_CONFIG:num.io.threads=8, DEFAULT_CONFIG:num.io.threads=8}
```

### A.2 Modifier une configuration à chaud

Certains paramètres sont **dynamiques** (*read-only*, *per-broker* ou *cluster-wide* : voir la colonne
« Dynamic Update Mode » de la documentation des brokers).

```bash
# Pour un seul broker
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 11 --alter --add-config num.io.threads=12
# Pour tous les brokers (valeur par défaut dynamique du cluster)
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-default --alter --add-config log.cleaner.threads=2
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 11 --describe
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-default --describe
docker compose logs broker-1 --since 2m | grep -i "num.io.threads" | head -3
```

```text
Completed updating config for broker 11.
Completed updating default config for brokers in the cluster.
Dynamic configs for broker 11 are:
  num.io.threads=12 sensitive=false synonyms={DYNAMIC_BROKER_CONFIG:num.io.threads=12, STATIC_BROKER_CONFIG:num.io.threads=8, DEFAULT_CONFIG:num.io.threads=8}
Default configs for brokers in the cluster are:
  log.cleaner.threads=2 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.cleaner.threads=2}
```

Un paramètre statique ne peut pas être modifié ainsi :

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 11 --alter --add-config log.dirs=/tmp/x
```

```text
Error while executing config command with args ...
... Cannot update these configs dynamically: [log.dirs]
```

Retour à la configuration initiale :

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 11 --alter --delete-config num.io.threads
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-default --alter --delete-config log.cleaner.threads
```

> Les configurations dynamiques sont stockées dans le **journal des métadonnées KRaft** : elles survivent
> aux redémarrages et sont appliquées à tout nouveau broker. Elles priment sur `server.properties`.

### A.3 Inspecter le journal des métadonnées (sur un contrôleur)

```bash
docker exec -e KAFKA_OPTS= controller-1 /opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log | grep -E '"type":"(TOPIC|CONFIG|REGISTER_BROKER)_RECORD"' | head -5
```

```text
| offset: ... payload: {"type":"REGISTER_BROKER_RECORD","version":...,"data":{"brokerId":11,...,"rack":"rack-a",...}}
| offset: ... payload: {"type":"TOPIC_RECORD","version":0,"data":{"name":"orders","topicId":"..."}}
| offset: ... payload: {"type":"CONFIG_RECORD","version":0,"data":{"resourceType":4,"resourceName":"11","name":"num.io.threads","value":"12"}}
```

> `-e KAFKA_OPTS=` vide la variable qui charge l'agent JMX (son port 9404 est déjà utilisé par le
> processus du contrôleur) : à utiliser pour **tout** outil lancé par `docker exec` dans un noeud Kafka.

---

## Partie B – Configuration et administration des topics

### B.1 Rétention et segments

| Paramètre (topic) | Défaut | Rôle |
|---|---|---|
| `retention.ms` | 7 jours (`log.retention.hours=168`) | un segment est supprimé quand son message **le plus récent** a dépassé cet âge |
| `retention.bytes` | -1 (illimité) | taille maximale **par partition** |
| `segment.bytes` / `segment.ms` | 1 Gio / 7 jours | fermeture (« roll ») du segment actif |
| `cleanup.policy` | `delete` | `delete`, `compact` ou `compact,delete` |
| `max.message.bytes` | ~1 Mo | taille max d'un lot de messages |
| `min.insync.replicas` | valeur du broker | durabilité en `acks=all` |

> La suppression se fait **par segment entier**, lors d'une vérification périodique
> (`log.retention.check.interval.ms`, 5 min ici). Pour la rétention par durée, le segment **actif** est
> supprimé lui aussi quand tous ses messages ont expiré : Kafka crée d'abord un nouveau segment vide.
> Les offsets ne repartent **jamais** à 0. (La compaction, elle, ne touche jamais le segment actif.)

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic retention-demo --partitions 1 --replication-factor 3 \
  --config segment.ms=10000 --config retention.ms=60000
for i in $(seq 1 5); do echo "message $i - $(date +%T)"; done | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic retention-demo
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic retention-demo --time earliest
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic retention-demo --time latest
```

```text
Created topic retention-demo.
retention-demo:0:0
retention-demo:0:5
```

Après plus de 10 s (`segment.ms`), un nouveau message **ferme** le segment courant et en ouvre un
nouveau (le « roll » a lieu à l'écriture) :

```bash
sleep 15
echo "message tardif - $(date +%T)" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic retention-demo
docker exec broker-1 ls /var/lib/kafka/data/retention-demo-0 | grep '\.log'
```

```text
00000000000000000000.log
00000000000000000005.log
```

Deux segments : `...000.log` (fermé, offsets 0 à 4) et `...005.log` (actif, offset 5). Le nom d'un
segment est l'offset de son premier message.

Attendez maintenant la prochaine vérification de rétention (au plus ~6 min). La boucle s'arrête dès
que le début de la partition a avancé (vous pouvez passer à la suite dans un autre terminal) :

```bash
until kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic retention-demo --time earliest | grep -q ':6$'; do sleep 20; done
date +%T
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic retention-demo --time earliest
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic retention-demo --time latest
docker exec broker-1 ls /var/lib/kafka/data/retention-demo-0 | grep '\.log'
```

```text
20:51:37
retention-demo:0:6
retention-demo:0:6
00000000000000000000.log.deleted
00000000000000000005.log.deleted
00000000000000000006.log
```

Lecture du résultat :

- `earliest = latest = 6` : la partition est **vide**, les 6 messages ont été supprimés. Lors de la
  vérification, **tous** avaient plus de 60 s, y compris le « message tardif » : le segment actif a donc
  été supprimé lui aussi, après création d'un nouveau segment vide `...006.log` ;
- le prochain message recevra l'offset **6** : les offsets ne sont jamais réutilisés ;
- les fichiers `*.deleted` disparaissent après `file.delete.delay.ms` (60 s par défaut) ;
- en production, avec `retention.ms` = 7 jours, les messages récents sont toujours dans un segment
  non expiré : seuls les segments les plus anciens disparaissent, un par un.

> **Pour voir une suppression partielle** (segments anciens supprimés, segment récent conservé), il
> faut qu'un message récent existe au moment de la vérification. Laissez tourner quelques minutes :
> `while true; do echo "flux $(date +%T)"; sleep 20; done | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic retention-demo`
> (arrêt par `Ctrl+C`) : à chaque vérification, `earliest` avance mais ne rejoint jamais `latest`.

> **Rejouer la démonstration :** supprimez d'abord le topic, puis attendez quelques secondes avant de le
> recréer : `kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --if-exists --topic retention-demo && sleep 5`.

### B.2 Purger un topic

**Méthode recommandée** – supprimer les messages jusqu'à un offset (`-1` = jusqu'au dernier) :

```bash
cat > /tmp/purge.json <<'EOF'
{"partitions": [{"topic": "test-formation", "partition": 0, "offset": -1},
                {"topic": "test-formation", "partition": 1, "offset": -1},
                {"topic": "test-formation", "partition": 2, "offset": -1}], "version": 1}
EOF
kafka-delete-records.sh --bootstrap-server $BOOTSTRAP --offset-json-file /tmp/purge.json
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic test-formation --time earliest
```

```text
Records delete operation completed:
partition: test-formation-0	low_watermark: 37
...
```

> Ancienne astuce (à éviter) : `retention.ms=1000` puis suppression de la surcharge (`--delete-config retention.ms`).

### B.3 Compaction : ne garder que la dernière valeur par clé

Le topic `customers-compacted` a été créé par `scripts/create-topics.sh` avec
`cleanup.policy=compact`, `segment.ms=10000`, `min.cleanable.dirty.ratio=0.01`, `delete.retention.ms=1000`
(valeurs agressives **pour la démonstration**).

Mises à jour successives de fiches clients (`NULL` = *tombstone* : suppression de la clé) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p data
cat > data/customers-updates.txt <<'EOF_KAFKA_KRAFT'
C-001:{"name":"Alice Martin","city":"Paris","loyalty":"STANDARD"}
C-002:{"name":"Bruno Bernard","city":"Lyon","loyalty":"STANDARD"}
C-003:{"name":"Chloe Dubois","city":"Marseille","loyalty":"SILVER"}
C-001:{"name":"Alice Martin","city":"Paris","loyalty":"SILVER"}
C-002:{"name":"Bruno Bernard","city":"Grenoble","loyalty":"STANDARD"}
C-001:{"name":"Alice Martin","city":"Versailles","loyalty":"GOLD"}
C-004:{"name":"David Thomas","city":"Toulouse","loyalty":"STANDARD"}
C-003:NULL
EOF_KAFKA_KRAFT
```

```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic customers-compacted \
  --reader-property parse.key=true --reader-property key.separator=: --reader-property null.marker=NULL \
  < ~/KAFKA-KRAFT/data/customers-updates.txt
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic customers-compacted --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true
```

Avant compaction : les **8** enregistrements sont présents. Attendez 15 s, envoyez un message pour fermer
le segment, attendez ~30 s (passage du *log cleaner*) puis relisez :

```bash
sleep 15
echo "C-005:{\"name\":\"Emma Robert\",\"city\":\"Nantes\",\"loyalty\":\"GOLD\"}" | kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP --topic customers-compacted --reader-property parse.key=true --reader-property key.separator=:
sleep 30
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic customers-compacted --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true
```

```text
Offset:4	C-002	{"name":"Bruno Bernard","city":"Grenoble","loyalty":"STANDARD"}
Offset:5	C-001	{"name":"Alice Martin","city":"Versailles","loyalty":"GOLD"}
Offset:6	C-004	{"name":"David Thomas","city":"Toulouse","loyalty":"STANDARD"}
Offset:7	C-003	null
Offset:8	C-005	{"name":"Emma Robert","city":"Nantes","loyalty":"GOLD"}
```

- Une seule valeur (la dernière) par clé ; les **offsets sont conservés** (trous dans la numérotation)
- La valeur de `C-003` a disparu ; il reste sa *tombstone* (`null`), qui sera elle-même supprimée lors
  d'une **prochaine** passe du *cleaner* (après `delete.retention.ms`), lorsque de nouvelles données arriveront
- Si toutes les valeurs sont encore là, patientez 30 s et relancez la lecture (le cleaner est asynchrone)

> Usages : état courant d'entités (référentiels, `__consumer_offsets`, topics internes de Kafka Connect et
> Kafka Streams *changelog*), tables ksqlDB.

### B.4 Grands messages 

La limite par défaut est **~1 Mo** par lot. Trois réglages doivent être alignés :
producer `max.request.size`, topic `max.message.bytes` (broker : `message.max.bytes`), et éventuellement
consumer `max.partition.fetch.bytes` / `fetch.max.bytes` (non bloquants depuis KIP-74 : un lot plus grand
est quand même retourné, idem pour `replica.fetch.max.bytes` entre brokers).

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic large-message --partitions 1 --replication-factor 3
head -c 2000000 /dev/urandom | base64 -w0 > /tmp/big-message.txt && echo >> /tmp/big-message.txt
ls -l /tmp/big-message.txt
```

1) Sans réglage : refus **par le producer** (2,6 Mo > `max.request.size`) :

```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic large-message < /tmp/big-message.txt
```

```text
ERROR Error when sending message to topic large-message with key: null, value: 2666668 bytes with error: ...
org.apache.kafka.common.errors.RecordTooLargeException: The message is 2666754 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.
```

2) Producer autorisé à 5 Mo : refus **par le broker** :

```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic large-message --command-property max.request.size=5242880 < /tmp/big-message.txt
```

```text
org.apache.kafka.common.errors.RecordTooLargeException: The request included a message larger than the max message size the server will accept.
```

3) Topic autorisé à 5 Mo : succès (préférer un réglage **par topic** plutôt que global) :

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name large-message --alter --add-config max.message.bytes=5242880
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic large-message --command-property max.request.size=5242880 < /tmp/big-message.txt
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic large-message --from-beginning --max-messages 1 | wc -c
```

```text
2666669
```

> Au-delà de quelques Mo : stocker l'objet dans un stockage externe (S3, HDFS...) et ne publier dans
> Kafka qu'une **référence** (pattern *claim check*).

---

## Partie C – Quotas :

Les quotas protègent le cluster contre un client qui monopoliserait les ressources (*noisy neighbour*).

| Type de quota | Unité | Ce qui est limité |
|---|---|---|
| `producer_byte_rate` | octets/s | débit d'écriture d'un client |
| `consumer_byte_rate` | octets/s | débit de lecture d'un client |
| `request_percentage` | % d'un thread | temps passé par les threads réseau et I/O du broker pour ce client |
| `controller_mutation_rate` | partitions/s | créations / suppressions de partitions (protège le contrôleur) |

**Entités :**
- `users` (principal **authentifié** : SASL, mTLS),
- `clients` (`client.id`, **déclaratif**),
- combinaison des deux,
- ou valeurs par défaut (`--entity-default`).

Sur nos listeners PLAINTEXT, tous les clients ont le principal `ANONYMOUS` : seuls les quotas par `client.id` sont utilisables ici.

> ⚠️ Les quotas de débit sont appliqués **par broker** : un client limité à 100 Kio/s peut écrire
> 100 Kio/s sur **chaque** broker leader des partitions qu'il utilise. D'où un topic à **une seule
> partition** pour la démonstration : tout le trafic passe par un seul broker.

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic quota-demo --partitions 1 --replication-factor 3
```
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name perf-quota \
  --add-config 'producer_byte_rate=102400,consumer_byte_rate=204800'
```
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type clients --entity-name perf-quota
```

```text
Created topic quota-demo.
Completed updating config for client perf-quota.
Quota configs for client-id 'perf-quota' are consumer_byte_rate=204800.0, producer_byte_rate=102400.0
```

Le quota est enregistré dans les métadonnées KRaft et appliqué **immédiatement** par tous les brokers,
sans redémarrage.

Comparaison d'un client limité et d'un client libre (3 000 messages de 1 000 octets, soit ~3 Mo).
`kafka-producer-perf-test.sh` affiche le débit **toutes les 5 s**, puis la moyenne ; `--print-metrics`
affiche en fin de test les métriques du producer, dont le temps de *throttling* :

```bash
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic quota-demo --num-records 3000 --record-size 1000 \
  --throughput -1 --command-property client.id=perf-quota acks=all --print-metrics | grep -E 'records sent|produce-throttle-time-max'
```
```bash
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic quota-demo --num-records 3000 --record-size 1000 \
  --throughput -1 --command-property client.id=perf-libre acks=all --print-metrics | grep -E 'records sent|produce-throttle-time-max'
```

Résultat (valeurs indicatives) :

```text
1412 records sent, 282.2 records/sec (0.27 MB/sec), ...    <- démarrage : « crédit » initial de ~1 Mo
505 records sent, 100.9 records/sec (0.10 MB/sec), ...     <- régime établi : ~100 Kio/s
500 records sent, 99.9 records/sec (0.10 MB/sec), ...
500 records sent, 100.0 records/sec (0.10 MB/sec), ...
3000 records sent, 142.6 records/sec (0.14 MB/sec), ...    <- moyenne sur ~21 s
producer-metrics:produce-throttle-time-max:{client-id=perf-quota}   : 412.000

3000 records sent, 2985.1 records/sec (2.85 MB/sec), ...   <- client "perf-libre" (dépend du poste)
producer-metrics:produce-throttle-time-max:{client-id=perf-libre}   : 0.000
```

Comment lire ce résultat :

- **régime établi** : les lignes intermédiaires du client `perf-quota` sont à ~0,10 Mo/s, soit le quota
  de 102 400 octets/s ;
- **crédit initial** : le broker mesure le débit sur une fenêtre glissante de ~10 s
  (`quota.window.num=11` × `quota.window.size.seconds=1`) et considère, au démarrage, que 10 s se sont
  déjà écoulées. Un nouveau client peut donc envoyer ~10 s de quota (≈ 1 Mo) d'un coup : la moyenne
  finale (0,14 Mo/s) est supérieure au quota. Plus le test est long, plus elle s'en rapproche ;
- **throttling** : le broker ne rejette pas les requêtes et ne perd aucun message. Il calcule un temps
  d'attente, le renvoie au client (`throttle_time_ms`) et suspend la connexion pendant ce délai :
  `produce-throttle-time-max` > 0 côté client, MBean `kafka.server:type=Produce,client-id=perf-quota`
  (attributs `byte-rate`, `throttle-time`) côté broker. Le quota `consumer_byte_rate` fonctionne de la
  même façon sur les lectures (`fetch-throttle-time-*`, MBean `type=Fetch`).

**Le quota est appliqué par broker** : relancez le test du client `perf-quota` sur `test-formation`
(3 partitions, leaders sur les 3 brokers). Le débit est nettement plus élevé : chaque broker accorde
100 Kio/s, et le crédit initial de chacun (~1 Mo) couvre presque à lui seul le tiers des 3 Mo qu'il reçoit.

```bash
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic test-formation --num-records 3000 --record-size 1000 \
  --throughput -1 --command-property client.id=perf-quota acks=all | tail -1
```

Nettoyage :

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name perf-quota \
  --delete-config producer_byte_rate,consumer_byte_rate
```
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic quota-demo
```

> **Ordre de priorité (du plus spécifique au plus général).** Pour un client, le broker applique la
> **première** entrée qui existe dans cette liste (les quotas ne se cumulent pas) :
>
> 1. `/users/<u>/clients/<c>`
> 2. `/users/<u>/clients/<défaut>`
> 3. `/users/<u>`
> 4. `/users/<défaut>/clients/<c>`
> 5. `/users/<défaut>/clients/<défaut>`
> 6. `/users/<défaut>`
> 7. `/clients/<c>`
> 8. `/clients/<défaut>`
>
> En production : un quota **par défaut** comme filet de sécurité
> (`kafka-configs.sh --alter --entity-type clients --entity-default --add-config producer_byte_rate=...`),
> des quotas par **utilisateur authentifié** (un `client.id` peut être choisi librement par n'importe
> quel client), et une alerte sur le temps de *throttling*. Le trafic de réplication entre brokers n'est
> pas concerné : il a ses propres limites (`leader/follower.replication.throttled.rate`, cf. TP02).

---

## Partie D – Redémarrage à chaud et mises à jour

### D.1 Rolling restart (un noeud à la fois)

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/rolling-restart.sh <<'EOF_KAFKA_KRAFT'
#!/usr/bin/env bash
# =============================================================================
#  rolling-restart.sh - Redémarrage "à chaud" (rolling restart) des brokers :
#  un broker à la fois, en attendant le retour à 0 partition sous-répliquée.
#  Procédure identique pour une montée de version (changer KAFKA_IMAGE dans .env
#  puis utiliser "docker compose up -d <broker>" au lieu de "restart").
#  Usage : ./scripts/rolling-restart.sh [restart|recreate]
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/kafka-env.sh >/dev/null
MODE=${1:-restart}
SCOPE=${2:-brokers}

wait_healthy() {
  local c=$1
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" = "healthy" ] && return 0
    sleep 5
  done
  echo "Timeout : $c n'est pas redevenu healthy"; exit 1
}

wait_no_urp() {
  for _ in $(seq 1 60); do
    if [ -z "$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null)" ]; then
      return 0
    fi
    sleep 5
  done
  echo "Timeout : des partitions sont toujours sous-répliquées"; exit 1
}

restart_node() {
  if [ "$MODE" = "recreate" ]; then
    docker compose up -d --no-deps --force-recreate "$1"
  else
    docker compose restart "$1"
  fi
}

if [ "$SCOPE" = "all" ]; then
  for controller in controller-1 controller-2 controller-3; do
    echo "=== $(date +%T) ${controller} : ${MODE} ==="
    restart_node "$controller"
    wait_healthy "$controller"
    sleep 10   # le contrôleur rattrape le journal des métadonnées
    kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status | grep -E "LeaderId|MaxFollowerLag:"
  done
fi

for broker in broker-1 broker-2 broker-3; do
  echo "=== $(date +%T) ${broker} : ${MODE} ==="
  restart_node "$broker"
  wait_healthy "$broker"
  echo "    ${broker} healthy, attente de la resynchronisation des réplicas..."
  wait_no_urp
  echo "    0 partition sous-répliquée : noeud suivant"
done
echo "=== Rolling restart terminé ==="
EOF_KAFKA_KRAFT
chmod +x scripts/rolling-restart.sh
```

Laissez un producteur tourner pendant l'opération (aucune erreur attendue) puis lancez le redémarrage
des brokers :

```bash
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic orders-audit --num-records 100000 --record-size 200 \
  --throughput 20 --reporting-interval 10000 --command-property acks=all &
./scripts/rolling-restart.sh restart brokers
kill %1
```

```text
=== 10:42:01 broker-1 : restart ===
    broker-1 healthy, attente de la resynchronisation des réplicas...
    0 partition sous-répliquée : noeud suivant
=== 10:43:10 broker-2 : restart ===
...
=== Rolling restart terminé ===
```

> L'arrêt d'un broker (`SIGTERM`) déclenche un **arrêt contrôlé** : il transfère ses partitions leader
> avant de s'arrêter. D'où `stop_grace_period: 60s` dans Compose (Docker tue sinon le processus au bout de 10 s).

### D.2 Montée de version (procédure)

1. Lire les notes de version (*Upgrade notes*) ; vérifier la compatibilité des clients (bidirectionnelle
   depuis Kafka 0.10.2 ; Kafka 4.x exige des clients ≥ 2.1).
2. **Contrôleurs** d'abord, puis **brokers**, **un par un**, en attendant 0 partition sous-répliquée.
3. Une fois le comportement validé, **finaliser** la version des métadonnées (irréversible si la nouvelle
   version contient des changements de métadonnées).

Avec ce projet (exemple de passage de 4.3.2 à 4.4.0) :

```bash
# 1. modifier KAFKA_IMAGE dans .env
# 2. recréer les conteneurs un par un avec la nouvelle image
./scripts/rolling-restart.sh recreate all
# 3. finaliser
kafka-features.sh --bootstrap-server $BOOTSTRAP upgrade --release-version 4.4 --dry-run
kafka-features.sh --bootstrap-server $BOOTSTRAP upgrade --release-version 4.4
```

Etat actuel des *features* du cluster :

```bash
kafka-features.sh --bootstrap-server $BOOTSTRAP describe
kafka-features.sh --bootstrap-server $BOOTSTRAP upgrade --release-version 4.4 --dry-run
```

```text
Feature: metadata.version   SupportedMinVersion: 3.3-IV3   SupportedMaxVersion: 4.4-IV2   FinalizedVersionLevel: 4.4-IV2   Epoch: ...
...
metadata.version can be upgraded to ...        <- --dry-run : validation sans modification
```

---

## Partie E – Automatisation du déploiement et de l'administration

| Besoin | Outils |
|---|---|
| Installer / configurer des brokers | Ansible (rôles communautaires, *cp-ansible*), Terraform + images, **Helm / opérateur Strimzi** (Kubernetes) |
| Topics, ACL, quotas "as code" | scripts **idempotents** (`--if-not-exists`), API Admin (TP03), `NewTopic` Spring (TP03), CRD Strimzi `KafkaTopic` / `KafkaUser`, provider Terraform Kafka, JulieOps |
| Rééquilibrage | Cruise Control (intégré à Strimzi) |
| Contrôles | `scripts/cluster-status.sh`, supervision (TP05) |

Exemple : création déclarative des topics depuis un fichier CSV versionné (Git) – exécutable N fois :

```bash
./scripts/create-topics.sh
./scripts/create-topics.sh     # 2e exécution : aucune erreur, rien n'est modifié
```

Pour information, l'équivalent Kubernetes avec l'opérateur Strimzi (utilisé par les équipes qui
déploient Kafka sur Kubernetes) :

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: orders
  labels:
    strimzi.io/cluster: kafka-kraft
spec:
  partitions: 6
  replicas: 3
  config:
    min.insync.replicas: 2
    retention.ms: 604800000
```

---

## Partie F – Configuration réseau et Kafka dans Docker

```text
 Hôte WSL (CLI, Java)                         Réseau Docker "kafka-net"
 ─────────────────────                        ──────────────────────────────────────────────────
 bootstrap localhost:9092 ──(port publié)──►  broker-1  EXTERNAL://:9092   annonce localhost:9092
                                              broker-1  INTERNAL://:19092  annonce broker-1:19092 ◄── kafka-ui,
 ◄── métadonnées : brokers = localhost:9092, localhost:9094, localhost:9096                            connect...
```

Le client contacte **un** broker (*bootstrap*), reçoit la liste des brokers **telle qu'annoncée sur le
listener utilisé**, puis se connecte directement au leader de chaque partition.

**Expérience 1** – depuis un conteneur du réseau Docker, le listener INTERNAL fonctionne :

```bash
docker run --rm --network kafka-net apache/kafka:4.4.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --list | head -3
```

**Expérience 2** – depuis ce même conteneur, `localhost:9092` désigne... le conteneur lui-même :

```bash
printf 'default.api.timeout.ms=10000\nrequest.timeout.ms=5000\n' > /tmp/admin-timeout.properties
docker run --rm --network kafka-net -v /tmp/admin-timeout.properties:/tmp/admin.properties:ro apache/kafka:4.4.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /tmp/admin.properties --list
```

```text
... Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
Error while executing topic command : Timed out waiting for a node assignment. Call: listTopics
```

**Expérience 3** – depuis l'hôte, le nom `broker-1` est inconnu :

```bash
kafka-topics.sh --bootstrap-server broker-1:19092 --list
```

```text
... Couldn't resolve server broker-1:19092 from bootstrap.servers as DNS resolution failed for broker-1
```

> **Erreur classique** : annoncer `EXTERNAL://broker-1:9092`. Le *bootstrap* depuis l'hôte réussit
> (port publié), mais les connexions suivantes vers `broker-1` échouent : le client reste bloqué.
> Règle : **un listener par réseau**, chacun annonçant une adresse **résolvable et joignable** depuis ce réseau.

| Contexte | Adresse à annoncer |
|---|---|
| Conteneurs du même réseau Docker | `nom-du-service:port-interne` |
| Hôte WSL / Windows | `localhost:port-publié` |
| Autres machines | nom DNS ou IP de l'hôte Docker |
| Kubernetes | service *headless* par broker (Strimzi : listeners `internal`, `route`, `loadbalancer`, `nodeport`) |

---

## Partie G – Consoles d'administration web

### G.1 Kafka UI (kafbat) – <http://localhost:8080>

Réalisez dans l'interface :

1. *Brokers* : configuration du broker 11, métriques, répartition des partitions par broker
2. *Topics → Add a topic* : `ui-demo`, 3 partitions, RF 3, `retention.ms=86400000`
3. *Topics → ui-demo → Produce message* : clé `k1`, valeur `{"hello":"kafka"}`
4. *Topics → orders → Messages* : filtre sur la clé `C-001`, affichage depuis l'offset 0
5. *Consumers* : groupe `groupe-commandes`, lag par partition, *Reset offsets*
6. *Topics → ui-demo → Settings* : modifier `retention.ms`, puis supprimer le topic

### G.2 AKHQ (alternative) – <http://localhost:8090>

```bash
docker compose --profile consoles up -d akhq
sleep 20
docker compose --profile consoles ps akhq
```

Comparez : navigation dans les topics et les messages, consumer groups, configuration, ACL, schémas
(TP07c), connecteurs (TP07a). Autres consoles : Redpanda Console, Conduktor, Confluent Control Center.

```bash
docker compose --profile consoles stop akhq
```

### G.3 Stockage par broker (ligne de commande)

```bash
kafka-log-dirs.sh --bootstrap-server $BOOTSTRAP --describe --topic-list orders | grep '^{' \
  | jq -r '.brokers[] | .broker as $b | .logDirs[] | .partitions[] | "\($b)\t\(.partition)\t\(.size) octets"'
docker exec broker-1 du -sh /var/lib/kafka/data
```

---

## Synthèse – aide-mémoire 

| Action | Commande |
|---|---|
| Lister / décrire | `kafka-topics.sh --bootstrap-server $BOOTSTRAP --list` / `--describe --topic T` |
| Partitions en anomalie | `--describe --under-replicated-partitions` / `--under-min-isr-partitions` / `--unavailable-partitions` |
| Ajouter des partitions | `kafka-topics.sh ... --alter --topic T --partitions 6` |
| Config topic | `kafka-configs.sh ... --entity-type topics --entity-name T --alter --add-config k=v` |
| Purger | `kafka-delete-records.sh ... --offset-json-file f.json` |
| Offsets d'un topic | `kafka-get-offsets.sh ... --topic T --time earliest\|latest` |
| Groupes | `kafka-consumer-groups.sh ... --list` / `--describe --group G` / `--reset-offsets ...` |
| Quorum KRaft | `kafka-metadata-quorum.sh ... describe --status` / `--replication` |
| Brokers | `kafka-cluster.sh list-endpoints --bootstrap-server ...` |
| Features / versions | `kafka-features.sh ... describe` / `upgrade --release-version 4.4` |
| Elections | `kafka-leader-election.sh ... --election-type PREFERRED --all-topic-partitions` |
| Réaffectation | `kafka-reassign-partitions.sh ... --generate / --execute --throttle / --verify` |
| Lire un segment | `kafka-dump-log.sh --files X.log --print-data-log` |

### Questions de révision

1. Quelle est la différence entre `message.max.bytes` et `max.message.bytes` ?
2. Pourquoi la compaction ne s'applique-t-elle jamais au segment actif ?
3. Dans quel ordre mettre à jour contrôleurs et brokers ? Quand lancer `kafka-features.sh upgrade` ?
4. Un client Java sous Windows (hors WSL) peut-il utiliser `localhost:9092` ? Pourquoi ?

✅ **Suite : TP05 – Superviser et optimiser un cluster.**
