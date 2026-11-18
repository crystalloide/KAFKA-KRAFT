# `VipPartitioner.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/producer/VipPartitioner.java` · 42 lignes

Partitionneur personnalisé : il réserve la partition 0 aux clients VIP et répartit les autres clients sur les partitions restantes.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 8 · Package et imports

```java
 1  package fr.formation.kafka.producer;
 2  
 3  import org.apache.kafka.clients.producer.Partitioner;
 4  import org.apache.kafka.common.Cluster;
 5  import org.apache.kafka.common.utils.Utils;
 6  
 7  import java.util.Map;
 8  import java.util.Set;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Partitioner` : interface à implémenter pour choisir soi-même la partition de chaque message.
- **L4** — Importe `Cluster` : vue des métadonnées du cluster côté client (topics, partitions, nœuds).
- **L5** — Importe `Utils` : utilitaires internes de Kafka, dont la fonction de hachage `murmur2`.
- **L7** — Importe `Map` : interface des tables clé → valeur.
- **L8** — Importe `Set` : interface des ensembles (sans doublon).

## Lignes 10 à 15 · Documentation et déclaration de la classe

```java
10  /**
11   * Partitionneur personnalisé : les clients VIP (C-001, C-002) sont isolés dans
12   * la partition 0 ; les autres clés sont réparties (murmur2) sur les partitions 1..n-1.
13   * Activé par : partitioner.class=fr.formation.kafka.producer.VipPartitioner
14   */
15  public class VipPartitioner implements Partitioner {
```

- **L10–14** — Javadoc : les clients VIP (`C-001`, `C-002`) sont isolés dans la partition 0 ; les autres clés sont réparties par hachage `murmur2` sur les partitions 1 à n-1. Le partitionneur s'active par le paramètre `partitioner.class`.
- **L15** — Déclare la classe, qui implémente l'interface `Partitioner` de Kafka : elle doit donc fournir `partition`, `configure` et `close`.

## Ligne 17 · Liste des clients VIP

```java
17      private static final Set<String> VIP = Set.of("C-001", "C-002");
```

- **L17** — Ensemble constant et non modifiable des identifiants de clients VIP.

## Lignes 19 à 31 · Méthode partition() : choix de la partition

```java
19      @Override
20      public int partition(String topic, Object key, byte[] keyBytes,
21                           Object value, byte[] valueBytes, Cluster cluster) {
22          int partitions = cluster.partitionsForTopic(topic).size();
23          if (key == null || partitions < 2) {
24              return 0;
25          }
26          if (VIP.contains(key.toString())) {
27              return 0;
28          }
29          // même fonction de hachage que le partitionneur par défaut de Kafka
30          return 1 + Utils.toPositive(Utils.murmur2(keyBytes)) % (partitions - 1);
31      }
```

- **L19** — `@Override` : la méthode redéfinit celle de l'interface (le compilateur vérifie la signature).
- **L20–21** — Signature imposée par Kafka. Pour chaque message, le producteur fournit le topic, la clé et la valeur (objets et formes sérialisées) ainsi que les métadonnées du cluster ; la méthode renvoie le numéro de partition.
- **L22** — Lit dans les métadonnées le nombre de partitions du topic.
- **L23** — Cas particuliers : pas de clé, ou topic à une seule partition…
- **L24** — …le message va en partition 0 (on ne peut pas faire mieux).
- **L25** — Ferme le bloc ouvert ligne 23.
- **L26** — Si la clé, convertie en chaîne, est celle d'un client VIP…
- **L27** — …le message va dans la partition 0, réservée aux VIP.
- **L28** — Ferme le bloc ouvert ligne 26.
- **L29** — Commentaire : la fonction de hachage est celle du partitionneur par défaut de Kafka.
- **L30** — Autres clients : on calcule le hachage `murmur2` des octets de la clé, on le rend positif, on prend le reste de la division par (nombre de partitions − 1), ce qui donne une valeur de 0 à n−2, et on ajoute 1 pour obtenir une partition de 1 à n−1. Une même clé donne donc toujours la même partition.

**Rappel** : **modulo** (noté **%**) est l'opération mathématique qui calcule le reste d'une division entière.

La règle absolue du modulo est la suivante : lorsque l'on fait **% B**, le résultat est toujours un nombre entier compris entre **0 et B - 1**.

Exemple pour un topic de 4 partitions au total :

**partitions = 4**

**partitions - 1** = 4 - 1 = **3**

L'opération devient donc un **% 3**.

- **L31** — Ferme le bloc ouvert ligne 21.

## Lignes 33 à 42 · Méthodes configure() et close()

```java
33      @Override
34      public void configure(Map<String, ?> configs) {
35          // pas de configuration
36      }
37  
38      @Override
39      public void close() {
40          // rien à libérer
41      }
42  }
```

- **L33** — `@Override`.
- **L34** — `configure` reçoit la configuration du producteur à l'initialisation du partitionneur.
- **L35** — Commentaire : aucun paramètre à lire ici.
- **L36** — Ferme le bloc ouvert ligne 34.
- **L38** — `@Override`.
- **L39** — `close` est appelée à la fermeture du producteur.
- **L40** — Commentaire : aucune ressource à libérer.
- **L41** — Ferme le bloc ouvert ligne 39.
- **L42** — Ferme le bloc ouvert ligne 15.
