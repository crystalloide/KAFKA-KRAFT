# `KafkaSettings.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/common/KafkaSettings.java` · 49 lignes

Classe utilitaire partagée par tous les exemples Java natifs : elle fournit l'adresse du cluster Kafka, les propriétés communes à tous les clients et la localisation des fichiers de données des TP.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 7 · Package et imports

```java
 1  package fr.formation.kafka.common;
 2  
 3  import org.apache.kafka.clients.CommonClientConfigs;
 4  
 5  import java.nio.file.Files;
 6  import java.nio.file.Path;
 7  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.common`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `CommonClientConfigs` : constantes des paramètres communs à tous les clients Kafka (`bootstrap.servers`, `client.id`…).
- **L5** — Importe `Files` : méthodes utilitaires sur les fichiers (existence, lecture…).
- **L6** — Importe `Path` : chemin de fichier.
- **L7** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 9 à 16 · Documentation et déclaration de la classe

```java
 9  /**
10   * Paramètres communs à tous les exemples.
11   * <p>
12   * L'adresse du cluster peut être surchargée par la variable d'environnement
13   * KAFKA_BOOTSTRAP_SERVERS (utile si l'IDE tourne sous Windows ou pour viser
14   * le noeud "standalone" du TP01 : localhost:9192).
15   */
16  public final class KafkaSettings {
```

- **L9–15** — Commentaire Javadoc : la classe regroupe les paramètres communs ; l'adresse du cluster peut être remplacée par la variable d'environnement `KAFKA_BOOTSTRAP_SERVERS` (par exemple pour viser le nœud autonome du TP01 sur `localhost:9192`).
- **L16** — Déclare la classe publique `KafkaSettings`. `final` interdit d'en hériter : c'est une simple boîte à outils.

## Lignes 18 à 21 · Constante et constructeur privé

```java
18      public static final String DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096";
19  
20      private KafkaSettings() {
21      }
```

- **L18** — Constante publique : la liste d'amorçage par défaut, c'est-à-dire les trois brokers du cluster Docker des TP (ports 9092, 9094, 9096). Donner plusieurs adresses permet de se connecter même si un broker est arrêté.
- **L20** — Constructeur privé et vide : empêche d'instancier la classe, qui ne contient que des méthodes statiques.
- **L21** — Ferme le bloc ouvert ligne 20.

## Lignes 23 à 26 · Méthode bootstrapServers() : adresse du cluster

```java
23      public static String bootstrapServers() {
24          String env = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
25          return (env == null || env.isBlank()) ? DEFAULT_BOOTSTRAP : env;
26      }
```

- **L23** — Déclare la méthode statique qui renvoie la liste des brokers à contacter.
- **L24** — Lit la variable d'environnement `KAFKA_BOOTSTRAP_SERVERS` ; `System.getenv` renvoie `null` si elle n'existe pas.
- **L25** — Opérateur ternaire : si la variable est absente ou vide (`isBlank`), on renvoie la valeur par défaut, sinon la valeur de la variable.
- **L26** — Ferme le bloc ouvert ligne 23.

## Lignes 28 à 34 · Méthode base() : propriétés communes à tous les clients

```java
28      /** Propriétés de base : adresse du cluster et identifiant du client. */
29      public static Properties base(String clientId) {
30          Properties props = new Properties();
31          props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
32          props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
33          return props;
34      }
```

- **L28** — Javadoc : la méthode fournit l'adresse du cluster et l'identifiant du client.
- **L29** — Déclare la méthode statique `base`, qui reçoit l'identifiant du client et renvoie un objet `Properties`.
- **L30** — Crée la table de propriétés vide.
- **L31** — Renseigne `bootstrap.servers` : les brokers utilisés pour la première connexion. Le client découvre ensuite tout le cluster par lui-même.
- **L32** — Renseigne `client.id` : nom logique du client, visible dans les journaux, les métriques et les quotas des brokers.
- **L33** — Renvoie les propriétés ; chaque exemple y ajoute ensuite ses propres paramètres (sérialiseurs, groupe…).
- **L34** — Ferme le bloc ouvert ligne 29.

## Lignes 36 à 49 · Méthode dataFile() : retrouver un fichier de données

```java
36      /**
37       * Retrouve le fichier data/orders.csv du dépôt, que le programme soit lancé
38       * depuis java/kafka-java-native (run.sh) ou depuis la racine du dépôt (IDE).
39       */
40      public static Path dataFile(String name) {
41          for (String candidate : new String[]{"data/" + name, "../../data/" + name, "../data/" + name}) {
42              Path path = Path.of(candidate);
43              if (Files.exists(path)) {
44                  return path;
45              }
46          }
47          throw new IllegalStateException("Fichier data/" + name + " introuvable depuis " + Path.of("").toAbsolutePath());
48      }
49  }
```

- **L36–39** — Javadoc : la méthode retrouve un fichier du répertoire `data/` du dépôt, que le programme soit lancé depuis `java/kafka-java-native` (script `run.sh`) ou depuis la racine du dépôt (IDE).
- **L40** — Déclare la méthode statique `dataFile`, qui reçoit un nom de fichier et renvoie son chemin (`Path`).
- **L41** — Parcourt trois emplacements candidats, relatifs au répertoire courant : `data/`, `../../data/` et `../data/`.
- **L42** — Transforme la chaîne candidate en objet `Path`.
- **L43** — Teste si le fichier existe à cet emplacement.
- **L44** — Si oui, renvoie immédiatement ce chemin (la boucle s'arrête).
- **L45** — Ferme le bloc ouvert ligne 43.
- **L46** — Ferme le bloc ouvert ligne 41.
- **L47** — Aucun candidat n'existe : lève une `IllegalStateException` dont le message indique le fichier cherché et le répertoire courant absolu, pour faciliter le diagnostic.
- **L48** — Ferme le bloc ouvert ligne 40.
- **L49** — Ferme le bloc ouvert ligne 16.
