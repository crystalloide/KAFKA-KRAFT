# `StreamsSupport.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsSupport.java` · 60 lignes

Classe utilitaire des exemples Kafka Streams : elle construit la configuration commune et prend en charge le démarrage, la surveillance et l'arrêt propre d'une application.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 12 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.common.serialization.Serdes;
 5  import org.apache.kafka.streams.KafkaStreams;
 6  import org.apache.kafka.streams.StreamsBuilder;
 7  import org.apache.kafka.streams.StreamsConfig;
 8  import org.apache.kafka.streams.Topology;
 9  import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
10  
11  import java.util.Properties;
12  import java.util.concurrent.CountDownLatch;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `Serdes` : fabrique de *serdes* (sérialiseur + désérialiseur) pour les types courants.
- **L5** — Importe `KafkaStreams` : le moteur d'exécution d'une application Kafka Streams.
- **L6** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L7** — Importe `StreamsConfig` : constantes des noms de paramètres de Kafka Streams.
- **L8** — Importe `Topology` : le graphe de traitement (sources, processeurs, puits) construit par le `StreamsBuilder`.
- **L9** — Importe `StreamsUncaughtExceptionHandler` : gestionnaire des exceptions non interceptées dans les threads de traitement.
- **L11** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.
- **L12** — Importe `CountDownLatch` : verrou qui bloque un thread jusqu'à ce qu'un compteur atteigne zéro.

## Lignes 14 à 18 · Déclaration de la classe

```java
14  /** Démarrage commun des applications Kafka Streams des TP. */
15  final class StreamsSupport {
16  
17      private StreamsSupport() {
18      }
```

- **L14** — Javadoc : démarrage commun des applications Kafka Streams des TP.
- **L15** — Déclare la classe `StreamsSupport`, `final` (pas d'héritage) et sans modificateur `public` : elle n'est visible que dans son package.
- **L17** — Constructeur privé et vide : la classe ne contient que des méthodes statiques et ne s'instancie pas.
- **L18** — Ferme le bloc ouvert ligne 17.

## Lignes 20 à 34 · Méthode config() : configuration commune

```java
20      /**
21       * application.id = identifiant de l'application ; il sert de group.id, de préfixe
22       * aux topics internes (changelog, repartition) et au répertoire d'état local.
23       */
24      static Properties config(String applicationId) {
25          Properties props = new Properties();
26          props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
27          props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
28          props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
29          props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
30          props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
31          // relit le topic d'entrée depuis le début au premier démarrage
32          props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
33          return props;
34      }
```

- **L20–23** — Javadoc : `application.id` identifie l'application ; il sert de `group.id`, de préfixe aux topics internes (changelog, repartition) et de nom au répertoire d'état local.
- **L24** — Déclare la méthode `config`, qui reçoit l'identifiant de l'application et renvoie ses propriétés.
- **L25** — Crée la table de propriétés.
- **L26** — `application.id` : identifiant de l'application Kafka Streams. Toutes les instances lancées avec le même identifiant se partagent le travail.
- **L27** — `bootstrap.servers` : adresse du cluster, fournie par `KafkaSettings`.
- **L28** — `default.key.serde` : serde par défaut des clés, ici `String`. On passe le nom de la classe du serde.
- **L29** — `default.value.serde` : serde par défaut des valeurs, `String` également. Ils s'appliquent partout où la topologie ne précise pas d'autre serde.
- **L30** — `commit.interval.ms=1000` : validation des offsets et vidage des caches toutes les secondes, pour voir les résultats rapidement pendant les TP.
- **L31** — Commentaire : relire le topic d'entrée depuis le début au premier démarrage.
- **L32** — Paramètre destiné au consommateur interne de Kafka Streams : `consumerPrefix` produit la clé `consumer.auto.offset.reset`, réglée sur `earliest`.
- **L33** — Renvoie les propriétés.
- **L34** — Ferme le bloc ouvert ligne 24.

## Lignes 36 à 38 · Méthode run() : construction et affichage de la topologie

```java
36      static void run(StreamsBuilder builder, Properties props) {
37          Topology topology = builder.build();
38          System.out.println("Topologie :\n" + topology.describe());
```

- **L36** — Déclare la méthode `run`, qui reçoit le `StreamsBuilder` décrit par l'exemple et sa configuration.
- **L37** — Construit la topologie, c'est-à-dire le graphe de traitement.
- **L38** — Affiche sa description textuelle : sources, processeurs, puits, topics et *state stores*.

## Lignes 40 à 46 · Création du moteur et gestion des événements

```java
40          KafkaStreams streams = new KafkaStreams(topology, props);
41          streams.setUncaughtExceptionHandler(exception -> {
42              System.err.println("Erreur fatale du traitement : " + exception);
43              return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
44          });
45          streams.setStateListener((newState, oldState) ->
46                  System.out.println("Etat Kafka Streams : " + oldState + " -> " + newState));
```

- **L40** — Crée le moteur Kafka Streams à partir de la topologie et des propriétés. Rien n'est encore démarré.
- **L41** — Installe un gestionnaire pour les exceptions non interceptées des threads de traitement.
- **L42** — Affiche l'erreur sur la sortie d'erreur.
- **L43** — Renvoie la décision `SHUTDOWN_CLIENT` : cette instance de l'application s'arrête (les autres réponses possibles sont de remplacer le thread ou d'arrêter toutes les instances).
- **L44** — Ferme le bloc ouvert ligne 41.
- **L45** — Installe un écouteur des changements d'état du moteur (CREATED, REBALANCING, RUNNING…).
- **L46** — Affiche l'ancien et le nouvel état à chaque transition.

## Lignes 48 à 60 · Démarrage et attente de l'arrêt

```java
48          CountDownLatch latch = new CountDownLatch(1);
49          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
50              streams.close();
51              latch.countDown();
52          }));
53          streams.start();
54          try {
55              latch.await();
56          } catch (InterruptedException e) {
57              Thread.currentThread().interrupt();
58          }
59      }
60  }
```

- **L48** — Crée un verrou à compteur initialisé à 1 : il servira à bloquer le thread principal.
- **L49** — Enregistre un *shutdown hook* pour Ctrl+C.
- **L50** — Ferme proprement le moteur : validation des offsets, fermeture des *state stores*, sortie du groupe.
- **L51** — Libère le verrou.
- **L52** — Ferme le bloc ouvert ligne 49.
- **L53** — Démarre le moteur : les threads de traitement sont lancés en arrière-plan et l'appel rend la main aussitôt.
- **L54** — Ouvre un bloc `try`.
- **L55** — Bloque le thread principal jusqu'à ce que le verrou soit libéré, donc jusqu'à l'arrêt demandé.
- **L56** — Intercepte l'interruption éventuelle de l'attente.
- **L57** — Rétablit l'indicateur d'interruption du thread.
- **L58** — Ferme le bloc ouvert ligne 56.
- **L59** — Ferme le bloc ouvert ligne 36.
- **L60** — Ferme le bloc ouvert ligne 15.
