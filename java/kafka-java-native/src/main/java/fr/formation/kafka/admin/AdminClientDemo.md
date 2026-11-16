# `AdminClientDemo.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/admin/AdminClientDemo.java` · 60 lignes

Exemple d'utilisation de l'API Admin (TP03 / TP04) : décrire le cluster, créer un topic par programme s'il n'existe pas, puis afficher ses partitions et ses configurations.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 14 · Package et imports

```java
 1  package fr.formation.kafka.admin;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.admin.Admin;
 5  import org.apache.kafka.clients.admin.Config;
 6  import org.apache.kafka.clients.admin.DescribeClusterResult;
 7  import org.apache.kafka.clients.admin.NewTopic;
 8  import org.apache.kafka.clients.admin.TopicDescription;
 9  import org.apache.kafka.common.config.ConfigResource;
10  import org.apache.kafka.common.config.TopicConfig;
11  
12  import java.util.List;
13  import java.util.Map;
14  import java.util.Set;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.admin`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `Admin` : interface du client d'administration (topics, configurations, description du cluster).
- **L5** — Importe `Config` : ensemble des entrées de configuration d'une ressource (ici un topic).
- **L6** — Importe `DescribeClusterResult` : résultat (asynchrone) de la description du cluster : identifiant, nœuds, contrôleur.
- **L7** — Importe `NewTopic` : description d'un topic à créer : nom, nombre de partitions, facteur de réplication, configurations.
- **L8** — Importe `TopicDescription` : description d'un topic existant : partitions, leader, réplicas, ISR.
- **L9** — Importe `ConfigResource` : désigne la ressource (topic, broker…) dont on veut lire ou modifier la configuration.
- **L10** — Importe `TopicConfig` : constantes des noms de paramètres de topic (`retention.ms`, `min.insync.replicas`…).
- **L12** — Importe `List` : interface des listes.
- **L13** — Importe `Map` : interface des tables clé → valeur.
- **L14** — Importe `Set` : interface des ensembles (sans doublon).

## Lignes 16 à 21 · Documentation et déclaration de la classe

```java
16  /**
17   * TP03/TP04 - API Admin : décrire le cluster, créer et décrire un topic par programme
18   * (automatisation des déploiements, "topics as code").
19   * Usage : ./run.sh admin.AdminClientDemo [topic]
20   */
21  public class AdminClientDemo {
```

- **L16–20** — Javadoc : objectif du programme (automatiser les déploiements, « topics as code ») et commande de lancement `./run.sh admin.AdminClientDemo [topic]`.
- **L21** — Déclare la classe publique `AdminClientDemo`.

## Lignes 23 à 24 · Point d'entrée et paramètre

```java
23      public static void main(String[] args) throws Exception {
24          String topic = args.length > 0 ? args[0] : "admin-demo";
```

- **L23** — Méthode `main`. `throws Exception` : les appels `get()` sur les résultats asynchrones peuvent lever des exceptions vérifiées, qu'on laisse remonter.
- **L24** — Le nom du topic est le premier argument de la ligne de commande ; à défaut, `admin-demo`.

## Lignes 26 à 29 · Description du cluster

```java
26          try (Admin admin = Admin.create(KafkaSettings.base("admin-demo"))) {
27              DescribeClusterResult cluster = admin.describeCluster();
28              System.out.println("Cluster ID   : " + cluster.clusterId().get());
29              System.out.println("Brokers      : " + cluster.nodes().get());
```

- **L26** — Crée le client d'administration avec les propriétés de base (`client.id=admin-demo`). Le *try-with-resources* garantit sa fermeture en fin de bloc.
- **L27** — Demande la description du cluster. L'appel est asynchrone : il renvoie tout de suite un objet résultat contenant des « futurs ».
- **L28** — Affiche l'identifiant du cluster ; `get()` attend la réponse du broker.
- **L29** — Affiche la liste des brokers (identifiant, hôte, port, rack).

## Lignes 31 à 32 · Liste des topics

```java
31              Set<String> topics = admin.listTopics().names().get();
32              System.out.println("Topics       : " + topics);
```

- **L31** — Récupère l'ensemble des noms de topics du cluster (hors topics internes, par défaut).
- **L32** — L'affiche.

## Lignes 34 à 42 · Création du topic s'il n'existe pas

```java
34              if (!topics.contains(topic)) {
35                  NewTopic newTopic = new NewTopic(topic, 6, (short) 3)
36                          .configs(Map.of(
37                                  TopicConfig.RETENTION_MS_CONFIG, String.valueOf(3 * 24 * 3600 * 1000L),
38                                  TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
39                                  TopicConfig.COMPRESSION_TYPE_CONFIG, "producer"));
40                  admin.createTopics(List.of(newTopic)).all().get();
41                  System.out.println("Topic créé   : " + topic);
42              }
```

- **L34** — Ne crée le topic que s'il n'existe pas déjà : le programme peut ainsi être relancé sans erreur.
- **L35** — Décrit le topic à créer : 6 partitions et un facteur de réplication de 3 (le type attendu est `short`, d'où la conversion).
- **L36** — Lui associe des configurations propres au topic, données sous forme de table clé → valeur.
- **L37** — `retention.ms` : rétention de 3 jours, calculée en millisecondes. Le suffixe `L` force un calcul en `long` et évite un dépassement de capacité des entiers.
- **L38** — `min.insync.replicas=2` : une écriture `acks=all` exige au moins 2 réplicas à jour.
- **L39** — `compression.type=producer` : le broker conserve la compression choisie par le producteur.
- **L40** — Envoie la demande de création et attend sa fin : `all()` regroupe les résultats, `get()` bloque et lève une exception en cas d'échec.
- **L41** — Confirme la création.
- **L42** — Ferme le bloc ouvert ligne 34.

## Lignes 44 à 50 · Description des partitions du topic

```java
44              TopicDescription description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
45              System.out.println("\nDescription de " + topic + " :");
46              description.partitions().forEach(p -> System.out.printf(
47                      "  partition %d leader=%s réplicas=%s isr=%s%n",
48                      p.partition(), p.leader() == null ? "aucun" : p.leader().id(),
49                      p.replicas().stream().map(n -> n.id()).toList(),
50                      p.isr().stream().map(n -> n.id()).toList()));
```

- **L44** — Demande la description du topic, attend la réponse (table nom → description) et en extrait celle du topic voulu.
- **L45** — Affiche un titre (le `\n` insère une ligne vide avant).
- **L46** — Parcourt les partitions du topic et affiche une ligne formatée par partition.
- **L47** — Le format d'affichage : numéro de partition, leader, réplicas, ISR (`%n` = fin de ligne).
- **L48** — Premiers arguments : le numéro de la partition, puis l'identifiant du broker leader, ou « aucun » si la partition n'a plus de leader (partition hors ligne).
- **L49** — Liste des identifiants des brokers qui hébergent un réplica.
- **L50** — Liste des identifiants des réplicas synchronisés (ISR). Les parenthèses finales ferment `printf` et `forEach`.

## Lignes 52 à 60 · Configurations surchargées du topic

```java
52              ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
53              Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
54              System.out.println("\nConfigurations surchargées :");
55              config.entries().stream()
56                      .filter(e -> !e.isDefault())
57                      .forEach(e -> System.out.printf("  %s = %s%n", e.name(), e.value()));
58          }
59      }
60  }
```

- **L52** — Désigne la ressource dont on veut la configuration : le topic.
- **L53** — Demande ses configurations, attend la réponse et extrait celle de la ressource.
- **L54** — Affiche un titre.
- **L55** — Transforme les entrées de configuration en flux Java.
- **L56** — Ne garde que celles qui diffèrent de la valeur par défaut, c'est-à-dire celles définies explicitement pour ce topic.
- **L57** — Affiche chacune sous la forme `nom = valeur`.
- **L58** — Ferme le bloc ouvert ligne 26.
- **L59** — Ferme le bloc ouvert ligne 23.
- **L60** — Ferme le bloc ouvert ligne 21.
