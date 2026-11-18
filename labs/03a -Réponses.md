# Questions de révision : Apache Kafka 4.4.0

Voici les réponses détaillées aux quatre questions techniques sur le fonctionnement interne d'Apache Kafka (version 4.4.0).

---

### 1. Avec `enable.idempotence=true`, pourquoi `max.in.flight.requests.per.connection` doit-il être $\le 5$ ?

Lorsqu'un producteur idempotent envoie des lots de messages (*batches*) à un broker, il attribue à chaque lot un **numéro de séquence** (*Sequence Number*) strictement croissant par partition et par *Producer ID* (PID). 

Pour garantir qu'aucun message n'est dupliqué et que l'ordre est strictement préservé même en cas d'échec réseau et de nouvelle tentative (*retry*), le broker conserve en mémoire un historique glissant des **5 derniers numéros de séquence** traités avec succès pour chaque combinaison `(PID, Partition)`.

* **Si $\le 5$ requêtes sont en transit (*in-flight*) :** Le broker possède suffisamment d'historique pour détecter si un lot est un doublon ou s'il arrive dans le désordre (*OutOfOrderSequenceException*), permettant ainsi au producteur de réordonner et renvoyer les lots manquants en toute sécurité.
* **Si $> 5$ requêtes étaient autorisées :** Si la première requête échouait alors que les 5 suivantes réussissaient, le broker perdrait la trace du numéro de séquence initial dans son cache de 5 éléments. Il ne pourrait plus garantir l'ordre ni l'idempotence lors de la réémission, ce qui casserait la garantie *Exactly-Once* (EOS).

---

### 2. Que se passe-t-il si le traitement d'un lot dépasse `max.poll.interval.ms` ?

Le paramètre `max.poll.interval.ms` (par défaut 5 minutes) définit le délai maximum autorisé entre deux appels consécutifs à la méthode `poll()` par un consommateur. Il permet de détecter un consommateur "vivant" au niveau réseau (qui continue d'envoyer des *heartbeats*), mais bloqué dans son traitement applicatif (*livelock*).

Si le traitement d'un lot dépasse cette durée :
1. **Exclusion du groupe :** Le consommateur estime qu'il a dépassé le temps imparti et envoie volontairement une requête `LeaveGroup` au *Group Coordinator* (ou le coordinateur le considère comme mort/inactif).
2. **Déclenchement d'un Rebalance :** Le *Group Coordinator* déclenche un rééquilibrage (*rebalance*) du *Consumer Group* (selon le protocole KIP-848 en Kafka 4.x, les partitions sont réassignées de manière incrémentale aux autres membres actifs).
3. **Échec du commit d'offsets :** Lorsque le consommateur lent termine enfin son traitement et tente de valider ses offsets (`commitSync()` ou `commitAsync()`), le broker rejette la requête avec une exception **`CommitFailedException`** (ou `IllegalGenerationException` / `UnknownMemberIdException`).
4. **Risque de doublons :** Comme les offsets n'ont pas pu être validés, le nouveau consommateur ayant récupéré les partitions relira et retraitera les mêmes messages (sémantique *At-Least-Once*).

---

### 3. Pourquoi un consumer en `read_uncommitted` voit-il les messages d'une transaction annulée ?

Dans Kafka, les écritures transactionnelles sont **écrites directement dans le log de la partition** au fur et à mesure que le producteur les envoie, **avant même** que la transaction ne soit validée (`commit`) ou annulée (`abort`). 

* Lorsqu'une transaction se termine, le *Transaction Coordinator* écrit un marqueur de contrôle spécial (*Control Record* : `COMMIT` ou `ABORT`) à la fin du log.
* Kafka ne supprime jamais physiquement les messages d'une transaction annulée dans le log (ce serait trop coûteux en I/O disque).
* Le filtrage des messages annulés est donc **délégué au client consommateur** via le paramètre `isolation.level` :
  * En mode **`read_committed`**, le consommateur met en mémoire tampon les messages d'une transaction en cours et attend de lire le marqueur `COMMIT` avant de les exposer à l'application (ou les ignore s'il lit `ABORT`).
  * En mode **`read_uncommitted`** (valeur par défaut), le consommateur lit séquentiellement le log dès que les messages sont répliqués (*High Watermark*), sans attendre ni tenir compte des marqueurs transactionnels. Il expose donc immédiatement tous les messages, y compris ceux dont la transaction sera ultérieurement annulée.

---

### 4. Quelle est la différence entre un *consumer group* et un *share group* ?

Introduit pour apporter un modèle de consommation de type **file d'attente (*Queue*)** dans Kafka (via le KIP-932, stabilisé dans la branche Kafka 4.x), le **Share Group** diffère fondamentalement du **Consumer Group** classique sur la manière dont les partitions et les messages sont distribués.

| Critère | Consumer Group (Classique) | Share Group (Queues for Kafka) |
| :--- | :--- | :--- |
| **Modèle de distribution** | **Exclusif par partition.** Une partition ne peut être attribuée qu'à *un seul* consommateur du groupe à un instant $t$. | **Partagé par message.** Plusieurs consommateurs du même groupe peuvent lire simultanément la *même* partition. |
| **Parallélisme maximum** | Limité par le **nombre de partitions** du topic (les consommateurs excédentaires restent inactifs / *idle*). | **Indépendant du nombre de partitions.** Le parallélisme peut dépasser largement le nombre de partitions. |
| **Gestion de l'acquittement (Ack)** | **Par offset cumulatif.** Valider l'offset $N$ valide implicitement tous les messages précédents ($0$ à $N-1$). | **Par message individuel.** Chaque message est acquitté individuellement (`ACCEPT`, `RELEASE`, `REJECT`). |
| **Gestion des erreurs** | Un message bloquant bloque toute la partition (nécessite souvent une *Dead Letter Queue* gérée par le client). | Un message en échec peut être relâché (`RELEASE`) et retraité par un autre consommateur sans bloquer les messages suivants. |
| **Ordre des messages** | **Garanti** au sein de chaque partition. | **Non garanti**, puisque les messages d'une même partition sont traités en parallèle par différents consommateurs. |
