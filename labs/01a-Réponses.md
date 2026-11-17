# Questions de révision : Apache Kafka & Architecture KRaft

Ce document regroupe les questions essentielles et leurs réponses détaillées concernant le fonctionnement des partitions, des groupes de consommateurs et de l'architecture KRaft sous Apache Kafka.

---

### 1. Pourquoi l'ordre global d'un topic à 3 partitions n'est-il pas garanti ?

Kafka garantit l'ordre des messages **uniquement au niveau d'une partition individuelle**, et non à l'échelle globale d'un topic.

* Lorsqu'un topic possède **3 partitions**, les messages sont répartis entre ces 3 files d'attente (en fonction d'une clé de partitionnement ou d'une répartition de type *round-robin* / *sticky*).
* Puisque les messages s'écrivent et se lisent en parallèle sur ces 3 partitions indépendantes à des vitesses potentiellement différentes, l'ordre chronologique global entre toutes les partitions n'est plus garanti.

---

### 2. Que se passe-t-il si un 4e consumer rejoint un groupe qui lit un topic à 3 partitions ?

Dans un *Consumer Group* Kafka, **une partition ne peut être assignée qu'à un seul consumer à la fois** au sein du même groupe.

* Avec **3 partitions** et **4 consumers**, un rééquilibrage (*rebalance*) a lieu : les 3 premiers consumers se voient attribuer 1 partition chacun.
* Le **4e consumer se retrouvera inactif (*idle*)** : il ne recevra aucun message tant qu'un des autres consumers ne quitte pas le groupe ou tant que le nombre de partitions du topic n'est pas augmenté à 4 (ou plus).

---

### 3. Quelle commande permet de connaître le contrôleur actif d'un cluster KRaft ?

La commande dédiée pour interroger l'état du quorum de métadonnées KRaft est la suivante :

```bash
kafka-metadata-quorum.sh --bootstrap-server <broker_host:port> describe --status
```

*Dans le retour de la commande, le champ **`LeaderId`** indique l'identifiant du nœud contrôleur actif qui gère le quorum.*

---

### 4. Pourquoi faut-il conserver le `cluster.id` ?

Le `cluster.id` est un identifiant unique universel (UUID) généré lors du formatage initial des répertoires de stockage (`kafka-storage.sh format`). Il est indispensable pour plusieurs raisons :

* **Cohésion et identité du cluster** : Il garantit que seuls les brokers et contrôleurs partageant exactement ce même identifiant sont autorisés à communiquer et à appartenir au même cluster.
* **Intégrité des métadonnées** : En architecture KRaft, ce identifiant lie de manière stricte les journaux de métadonnées (`__cluster_metadata`) aux nœuds du quorum, empêchant qu'un serveur mal configuré ou réinitialisé avec un autre ID ne rejoigne le cluster par erreur et ne corrompe les données.