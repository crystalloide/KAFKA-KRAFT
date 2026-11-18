# Architecture et cycle de vie d'un message dans un Producer Kafka

Schéma résumé du cycle de vie d'un message (*record*) côté **Producer Kafka**, depuis son appel initial jusqu'à la confirmation de son écriture.

## Schéma du flux

```text
send(record) ─► Serializer ───► Partitioner ───────► RecordAccumulator (lots par partition) ───► Sender ───────────► broker leader
                (clé/valeur     (hash(clé) % N       batch.size, linger.ms,                      (acks, retries,            │
                 -> octets)      ou sticky)          compression.type, buffer.memory)            in-flight)                 ▼
                                                                                                                            │  
                                                                                                                  réplication vers l'ISR
callback(metadata, exception) ◄──────────────────────── acquittement ◄──────────────────────────────────────────────────────┘
```

---

## Explication détaillée étape par étape

### 1. `send(record)` ──► Serializer
* **Rôle :** Lorsque vous invoquez `producer.send(record)`, le message (composé d'une clé et d'une valeur sous forme d'objets ou de structures logiques) passe d'abord par le **Serializer**.
* **Fonctionnement :** Kafka ne comprend que les octets bruts (`byte[]`). Le sérialiseur convertit la clé et la valeur en tableaux d'octets (via `StringSerializer`, `JsonSerializer`, `Avro`, etc.) pour qu'ils puissent voyager sur le réseau et être stockés.

### 2. Partitioner
* **Rôle :** Décider dans quelle partition du topic le message doit être rangé.
* **Fonctionnement :** 
  * Si le message possède une **clé**, le partitionneur applique généralement une fonction de hachage ($\text{hash}(\text{clé}) \bmod N$, où $N$ est le nombre de partitions) pour garantir que les messages ayant la même clé vont toujours dans la même partition.
  * Si le message n'a **pas de clé**, les versions modernes de Kafka utilisent un partitionneur intelligent (comme le *Sticky Partitioner*) qui envoie les messages par lots successifs vers une même partition pour optimiser le regroupement, puis change de partition.

### 3. RecordAccumulator (lots par partition)
* **Rôle :** C'est un tampon mémoire (*buffer*) en arrière-plan qui accumule les messages par partition avant de les envoyer au réseau. Il évite de faire une requête réseau par message (ce qui serait catastrophique pour les performances).
* **Paramètres clés impliqués :**
  * `buffer.memory` : Taille totale de la mémoire tampon allouée au producer.
  * `batch.size` : Taille maximale (en octets) d'un lot (*batch*) pour une partition. Dès qu'un batch est plein, il est prêt à partir.
  * `linger.ms` : Temps d'attente maximal autorisé pour accumuler d'autres messages dans le batch, même si le `batch.size` n'est pas atteint.
  * `compression.type` : Type de compression appliqué au batch (ex. `gzip`, `snappy`, `lz4`, `zstd`) pour réduire la taille sur le réseau.

### 4. Sender (Thread en arrière-plan)
* **Rôle :** Un thread indépendant qui scrute en permanence le `RecordAccumulator` pour récupérer les batches prêts et les expédier sur le réseau vers les bons brokers.
* **Paramètres clés impliqués :**
  * `acks` : Détermine le niveau de garantie requis (`0` = pas d'attente, `1` = le leader a écrit, `all` = le leader et l'ISR ont écrit).
  * `retries` : Nombre de tentatives en cas d'erreur transitoire réseau ou broker.
  * `in-flight requests` : Nombre maximal de requêtes envoyées en parallèle sans attendre d'acquittement.

### 5. Broker Leader ──► Réplication vers l'ISR
* **Rôle :** Le broker leader de la partition reçoit le lot de messages envoyé par le `Sender`.
* **Fonctionnement :** Il écrit les messages dans son journal (*log*). Ensuite, les réplicas secondaires du groupe **ISR** (*In-Sync Replicas*) viennent chercher (*fetch*) ces nouveaux messages pour les dupliquer, garantissant ainsi la tolérance aux pannes.

### 6. Acquittement ◄── `callback(metadata, exception)`
* **Rôle :** Une fois que les conditions de l'attribut `acks` sont remplies (par exemple, écriture validée par le leader et l'ISR), un **acquittement** (*acknowledgment*) est renvoyé au `Sender`.
* **Fonctionnement :** Le thread principal ou le mécanisme de callback exécute alors votre `callback(metadata, exception)`. Si l'envoi a réussi, `metadata` contient le numéro de partition et l'offset assigné ; si une erreur fatale est survenue (après épuisement des `retries`), l'`exception` est remontée à votre application.