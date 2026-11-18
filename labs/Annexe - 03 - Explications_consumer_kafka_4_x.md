# Cycle de vie d'un Consumer Apache Kafka (Version 4.x)

Ce document valide et détaille le fonctionnement fondamental de la boucle de consommation, la gestion des offsets et le mécanisme de rééquilibrage (*rebalance*) dans Apache Kafka 4.x.

---

## Schéma récapitulatif

```text
subscribe(topics) ─► poll() ─► traitement ─► commit de l'offset (auto toutes les 5 s, ou commitSync/commitAsync)
        │                                                       │
        └─ group coordinator : rééquilibrage (rebalance) ◄──────┘  offsets stockés dans __consumer_offsets
           quand un consumer arrive / part / ne fait plus de poll() (max.poll.interval.ms)
```

---

## 1. La Boucle de Consommation

Le cycle d'exécution d'un consommateur repose sur quatre étapes séquentielles :

### `subscribe(topics)`
* **Rôle :** Le consommateur s'abonne à un ou plusieurs topics.
* **Fonctionnement :** Cette étape déclenche l'enregistrement du client auprès du cluster. Le broker désigné comme **Group Coordinator** prend en charge ce consommateur et lui attribue un ensemble de partitions via un protocole d'assignation.

### `poll()`
* **Rôle :** C'est le moteur principal du consommateur.
* **Fonctionnement :** En appelant `poll()`, le client récupère un lot de messages (*records*) depuis les partitions qui lui sont assignées.
* **Point critique :** Cet appel permet également au client de maintenir sa session active auprès du coordinateur.

### `traitement`
* **Rôle :** Exécution de la logique métier de votre application sur le lot de messages récupérés.

### `commit de l'offset`
* **Rôle :** Mémoriser la position de lecture (le dernier message traité avec succès pour chaque partition).
* **Deux modes disponibles :**
  * **Auto-commit (`enable.auto.commit = true`) :** Mode par défaut. Kafka commite automatiquement les offsets en arrière-plan à intervalle régulier (toutes les **5 secondes** par défaut, configurable via `auto.commit.interval.ms`).
  * **Commit manuel (`commitSync()` / `commitAsync()`) :** Offre un contrôle précis. Généralement appelé *après* la fin du traitement métier pour garantir la sémantique *At-Least-Once* (au moins une fois) et éviter toute perte de données en cas de crash applicatif.

---

## 2. Le Group Coordinator et le Rééquilibrage (*Rebalance*)

### Le Group Coordinator
Il s'agit d'un broker spécifique du cluster (élu en interne pour chaque *Consumer Group*) responsable de la gestion de l'état du groupe et de la coordination de l'attribution des partitions entre les membres actifs.

### Déclencheurs du rééquilibrage
Un *rebalance* redistribue les partitions entre les consommateurs disponibles dans trois situations principales :
1. **Arrivée d'un consommateur :** Ajout d'une nouvelle instance pour monter en charge (scalabilité horizontale).
2. **Départ d'un consommateur :** Arrêt propre d'une instance (ex: déploiement, réduction de charge).
3. **Dépassement de `max.poll.interval.ms` :** Si le traitement métier prend trop de temps (par défaut, plus de **5 minutes**) et empêche l'appel suivant à `poll()`, le coordinateur considère le consommateur comme bloqué ou mort. Il l'exclut alors du groupe et réattribue ses partitions aux autres membres.

---

## 3. Stockage des Offsets : `__consumer_offsets`

Dans **Kafka 4.x** (qui fonctionne exclusivement en mode **KRaft**, sans ZooKeeper), les positions de lecture sont gérées directement par le cluster :

* **Topic interne :** Les offsets sont enregistrés dans un topic système nommé `__consumer_offsets`.
* **Architecture :** Ce topic est hautement disponible, répliqué sur plusieurs brokers, et configuré avec une politique de nettoyage par compactage (*log compaction*). Cela permet à Kafka de ne conserver efficacement que la dernière position validée par chaque groupe pour chaque partition.

---

## Notes spécifiques à Kafka 4.x

* **Stabilité du modèle :** Le comportement illustré dans le schéma reste la norme pour la consommation standard basée sur des partitions et des offsets.
* **Évolutions notables :** Les versions récentes optimisent la vitesse des rééquilibrages (protocole KIP-848) et introduisent les **Share Groups**, un modèle alternatif permettant une consommation de type file d'attente (*queue*) avec acquittement individuel par message plutôt que par offset de partition.