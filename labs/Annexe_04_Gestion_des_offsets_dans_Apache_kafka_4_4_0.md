# Les Offsets dans Apache Kafka 4.4.0 : Plages de Valeurs et Inspection CLI

Dans **Apache Kafka 4.4.0**, la gestion des offsets repose sur deux notions complémentaires : la **plage théorique** imposée par le type de donnée dans le protocole Kafka, et la **plage active** réellement conservée dans le log d'une partition sur le broker.

---

## 1. Plage théorique des offsets

* **Type de donnée :** entier signé sur **64 bits** (`long` en Java / `INT64` dans le protocole binaire Kafka).
* **Plage valide pour les messages :**
  * **Minimum :** `0` (premier message écrit dans une partition).
  * **Maximum :** $2^{63} - 1$, soit **`9 223 372 036 854 775 807`** (`Long.MAX_VALUE`).
* **Valeurs négatives réservées (sentinelles) :**
  * `-1` : désigne *latest* (le prochain offset à produire) dans certaines requêtes d'API (ex. `ListOffsets`).
  * `-2` : désigne *earliest* (le plus ancien offset encore disponible).
  * *Remarque :* ces valeurs négatives ne correspondent jamais à des messages stockés.

---

## 2. Plage active réelle d'une partition (`[Log Start Offset, Log End Offset[`)

À un instant donné, une partition n'expose pas la totalité des offsets depuis `0`, mais uniquement la fenêtre encore présente sur disque :

* **Log Start Offset (*earliest*) :**
  * Borne inférieure de la partition.
  * Commence à `0`, puis avance au fur et à mesure que les anciens segments sont supprimés par la rétention (`retention.ms`, `retention.bytes`) ou tronqués explicitement (`deleteRecords`).
* **Log End Offset — LEO (*latest*) :**
  * Borne supérieure de la partition.
  * Correspond à l'offset qui sera attribué au **prochain** message produit (`dernier_offset_écrit + 1`).
* **High Watermark (HW) :**
  * Offset jusqu'auquel tous les réplicas synchronisés (*In-Sync Replicas*) ont répliqué les données. Un consommateur standard ne peut lire que jusqu'au *High Watermark*.

### Comportement et particularités
* **Out of Range :** Si un consommateur tente de lire un offset inférieur au `Log Start Offset` ou supérieur au `Log End Offset`, Kafka lève une `OffsetOutOfRangeException` (sauf si `auto.offset.reset` est configuré sur `earliest` ou `latest`).
* **Absence de remise à zéro (*wraparound*) :** Les offsets croissent de façon monotone. Même à un débit d'un million de messages par seconde sur une seule partition, il faudrait plus de $290\,000$ ans pour atteindre `Long.MAX_VALUE`.
* **Écarts (« trous ») dans la numérotation :** Certains offsets peuvent ne pas correspondre à des messages applicatifs visibles (notamment les *control records* utilisés par les transactions Kafka ou les segments nettoyés par *log compaction*).
* **Offset commité :** L'offset enregistré par un consumer group dans `__consumer_offsets` correspond toujours au **prochain offset à consommer** (`dernier_offset_traité + 1`).

---

## 3. Commandes CLI pour inspecter les offsets et le lag (Kafka 4.4.0)

Il n'existe pas une commande unique affichant simultanément le `Log Start Offset`, le `Log End Offset` et le `LAG`. On combine généralement deux outils fournis dans le répertoire `bin/` de Kafka.

### A. Récupérer le Log Start Offset et le Log End Offset (`kafka-get-offsets.sh`)

```bash
# Afficher le Log Start Offset (borne inférieure / earliest)
bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic mon-topic \
  --time -2

# Afficher le Log End Offset (borne supérieure / latest)
bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic mon-topic \
  --time -1
```

* **Filtrer sur une partition précise :** ajoutez `--partitions 0` (ou une liste séparée par des virgules : `--partitions 0,1,2`).
* **Format de sortie :** `TOPIC:PARTITION:OFFSET` (exemple : `mon-topic:0:1500`).

### B. Récupérer le Log End Offset et le Lag d'un Consumer Group (`kafka-consumer-groups.sh`)

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group mon-groupe \
  --describe
```

* **Colonnes importantes :**
  * `CURRENT-OFFSET` : prochain offset à lire pour ce groupe (dernier offset commité).
  * `LOG-END-OFFSET` : borne supérieure actuelle de la partition.
  * `LAG` : nombre de messages en retard (`LOG-END-OFFSET - CURRENT-OFFSET`).

### C. Synthèse pour une partition donnée

| Indicateur | Outil CLI | Option / Colonne |
| :--- | :--- | :--- |
| **Log Start Offset** | `kafka-get-offsets.sh` | `--time -2` |
| **Log End Offset** | `kafka-get-offsets.sh` / `kafka-consumer-groups.sh` | `--time -1` ou colonne `LOG-END-OFFSET` |
| **Current Offset** | `kafka-consumer-groups.sh` | Colonne `CURRENT-OFFSET` |
| **Lag** | `kafka-consumer-groups.sh` | Colonne `LAG` |

> **Note sur la sécurité :** Si votre cluster Kafka 4.4.0 utilise SSL/TLS ou SASL, ajoutez l'option `--command-config client.properties` aux commandes ci-dessus.