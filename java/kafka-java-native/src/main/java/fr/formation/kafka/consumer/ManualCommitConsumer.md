# `ManualCommitConsumer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/consumer/ManualCommitConsumer.java` · 110 lignes

Consommateur du TP03 à validation manuelle des offsets : validation asynchrone après chaque lot, synchrone à la fermeture et avant chaque perte de partition. Un message « crash » simule une panne avant validation.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 19 · Package et imports

```java
  1  package fr.formation.kafka.consumer;
  2  
  3  import fr.formation.kafka.common.KafkaSettings;
  4  import org.apache.kafka.clients.consumer.ConsumerConfig;
  5  import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
  6  import org.apache.kafka.clients.consumer.ConsumerRecord;
  7  import org.apache.kafka.clients.consumer.ConsumerRecords;
  8  import org.apache.kafka.clients.consumer.KafkaConsumer;
  9  import org.apache.kafka.clients.consumer.OffsetAndMetadata;
 10  import org.apache.kafka.common.TopicPartition;
 11  import org.apache.kafka.common.errors.WakeupException;
 12  import org.apache.kafka.common.serialization.StringDeserializer;
 13  
 14  import java.time.Duration;
 15  import java.util.Collection;
 16  import java.util.HashMap;
 17  import java.util.List;
 18  import java.util.Map;
 19  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.consumer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `ConsumerConfig` : constantes des noms de paramètres du consommateur.
- **L5** — Importe `ConsumerRebalanceListener` : interface de rappel invoquée quand des partitions sont retirées ou attribuées au consommateur.
- **L6** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L7** — Importe `ConsumerRecords` : le lot de messages rendu par un appel à `poll()`.
- **L8** — Importe `KafkaConsumer` : le client consommateur Kafka (non thread-safe : un consommateur par thread).
- **L9** — Importe `OffsetAndMetadata` : offset à valider (commit) pour une partition, avec une métadonnée optionnelle.
- **L10** — Importe `TopicPartition` : identifie une partition : couple (topic, numéro de partition).
- **L11** — Importe `WakeupException` : exception levée dans `poll()` quand un autre thread a appelé `consumer.wakeup()`.
- **L12** — Importe `StringDeserializer` : désérialiseur qui convertit des octets (UTF-8) en `String`.
- **L14** — Importe `Duration` : durée (utilisée pour les délais de `poll()` et les fenêtres).
- **L15** — Importe `Collection` : interface mère des collections.
- **L16** — Importe `HashMap` : table de hachage clé → valeur.
- **L17** — Importe `List` : interface des listes.
- **L18** — Importe `Map` : interface des tables clé → valeur.
- **L19** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 21 à 32 · Documentation et déclaration de la classe

```java
 21  /**
 22   * TP03 - Commit MANUEL des offsets ("at least once" maîtrisé).
 23   * <ul>
 24   *   <li>enable.auto.commit=false : l'application décide quand l'offset est commité</li>
 25   *   <li>commitAsync() après chaque lot traité, commitSync() à la fermeture</li>
 26   *   <li>ConsumerRebalanceListener : commit des offsets avant la perte d'une partition</li>
 27   * </ul>
 28   * Un message dont la valeur est "crash" simule une panne AVANT le commit : relancez le
 29   * programme, les messages non commités sont relus.
 30   * Usage : ./run.sh consumer.ManualCommitConsumer [topic] [groupe]
 31   */
 32  public class ManualCommitConsumer {
```

- **L21–31** — Javadoc : validation manuelle (« at least once » maîtrisé) avec `enable.auto.commit=false`, `commitAsync()` après chaque lot, `commitSync()` à la fermeture et dans le `ConsumerRebalanceListener`. Un message de valeur « crash » simule une panne avant la validation : au redémarrage, les messages non validés sont relus. Lancement : `./run.sh consumer.ManualCommitConsumer [topic] [groupe]`.
- **L32** — Déclare la classe publique `ManualCommitConsumer`.

## Lignes 34 à 36 · Point d'entrée et paramètres

```java
 34      public static void main(String[] args) {
 35          String topic = args.length > 0 ? args[0] : "test-formation";
 36          String group = args.length > 1 ? args[1] : "formation-manual-commit";
```

- **L34** — Méthode `main`.
- **L35** — Topic à lire : premier argument, ou `test-formation`.
- **L36** — Groupe : second argument, ou `formation-manual-commit`.

## Lignes 38 à 45 · Configuration du consommateur

```java
 38          Properties props = KafkaSettings.base("manual-commit-consumer");
 39          props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
 40          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 41          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 42          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 43          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
 44          props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
 45          props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
```

- **L38** — Propriétés de base avec `client.id=manual-commit-consumer`.
- **L39** — `group.id` : groupe de consommateurs.
- **L40** — Désérialiseur de la clé.
- **L41** — Désérialiseur de la valeur.
- **L42** — `auto.offset.reset=earliest` : lecture depuis le début en l'absence d'offset validé.
- **L43** — `enable.auto.commit=false` : c'est l'application qui décide quand un offset est validé.
- **L44** — `max.poll.records=10` : `poll()` rend au plus 10 messages (500 par défaut), pour de petits lots faciles à observer.
- **L45** — `max.poll.interval.ms=300000` : délai maximal de 5 minutes entre deux `poll()` ; au-delà, le consommateur est exclu du groupe (valeur par défaut, écrite ici pour la montrer).

## Lignes 47 à 48 · Création du consommateur et état à valider

```java
 47          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 48          Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();
```

- **L47** — Crée le consommateur.
- **L48** — Table des offsets traités mais pas encore validés : pour chaque partition, l'offset à valider.

## Lignes 50 à 58 · Arrêt propre sur Ctrl+C

```java
 50          Thread mainThread = Thread.currentThread();
 51          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
 52              consumer.wakeup();
 53              try {
 54                  mainThread.join();
 55              } catch (InterruptedException e) {
 56                  Thread.currentThread().interrupt();
 57              }
 58          }));
```

- **L50** — Mémorise le thread principal.
- **L51** — Enregistre un *shutdown hook*.
- **L52** — Interrompt le `poll()` en cours par une `WakeupException`.
- **L53** — Ouvre un bloc `try`.
- **L54** — Attend la fin du thread principal (dernier commit et fermeture).
- **L55** — Intercepte l'interruption éventuelle.
- **L56** — Rétablit l'indicateur d'interruption.
- **L57** — Ferme le bloc ouvert ligne 55.
- **L58** — Ferme le bloc ouvert ligne 51.

## Lignes 60 à 72 · Écouteur de rééquilibrage

```java
 60          ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
 61              @Override
 62              public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 63                  System.out.println(">>> Partitions retirées : " + partitions + " -> commit des offsets traités");
 64                  consumer.commitSync(pending);
 65                  pending.clear();
 66              }
 67  
 68              @Override
 69              public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 70                  System.out.println(">>> Partitions attribuées : " + partitions);
 71              }
 72          };
```

- **L60** — Crée, sous forme de classe anonyme, un écouteur des rééquilibrages du groupe.
- **L61** — `@Override`.
- **L62** — Méthode appelée avant que des partitions soient retirées à ce consommateur.
- **L63** — Affiche les partitions retirées.
- **L64** — Valide de façon synchrone les offsets déjà traités : le consommateur qui reprendra ces partitions ne relira pas ces messages.
- **L65** — Vide la table, puisque tout est validé.
- **L66** — Ferme le bloc ouvert ligne 62.
- **L68** — `@Override`.
- **L69** — Méthode appelée après l'attribution de nouvelles partitions.
- **L70** — Affiche les partitions reçues.
- **L71** — Ferme le bloc ouvert ligne 69.
- **L72** — Ferme le bloc ouvert ligne 60.

## Lignes 74 à 98 · Boucle de consommation

```java
 74          try {
 75              consumer.subscribe(List.of(topic), listener);
 76              while (true) {
 77                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
 78                  for (ConsumerRecord<String, String> record : records) {
 79                      if ("crash".equalsIgnoreCase(record.value())) {
 80                          System.out.println("!!! Panne simulée avant le commit de l'offset " + record.offset());
 81                          Runtime.getRuntime().halt(1);   // arrêt brutal : aucun commit
 82                      }
 83                      System.out.printf("traitement partition=%d offset=%d value=%s%n",
 84                              record.partition(), record.offset(), record.value());
 85                      // l'offset commité est celui du PROCHAIN message à lire
 86                      pending.put(new TopicPartition(record.topic(), record.partition()),
 87                              new OffsetAndMetadata(record.offset() + 1));
 88                  }
 89                  if (!records.isEmpty()) {
 90                      consumer.commitAsync(new HashMap<>(pending), (offsets, ex) -> {
 91                          if (ex != null) {
 92                              System.err.println("Echec du commit asynchrone : " + ex.getMessage());
 93                          } else {
 94                              System.out.println("offsets commités : " + offsets);
 95                          }
 96                      });
 97                  }
 98              }
```

- **L74** — Ouvre le bloc `try` qui encadre la consommation.
- **L75** — S'abonne au topic en fournissant l'écouteur de rééquilibrage.
- **L76** — Boucle infinie.
- **L77** — Récupère un lot de messages (attente d'une seconde au plus).
- **L78** — Parcourt les messages du lot.
- **L79** — Si la valeur du message est « crash » (sans tenir compte de la casse)…
- **L80** — …affiche l'offset concerné…
- **L81** — …et arrête brutalement la JVM avec `halt` : ni *shutdown hook* ni bloc `finally` ne sont exécutés, donc aucun offset n'est validé. C'est la simulation d'une panne.
- **L82** — Ferme le bloc ouvert ligne 79.
- **L83–84** — Traitement du message : ici, un simple affichage de la partition, de l'offset et de la valeur.
- **L85** — Commentaire : l'offset à valider est celui du prochain message à lire.
- **L86–87** — Enregistre pour cette partition l'offset du message traité + 1.
- **L88** — Ferme le bloc ouvert ligne 78.
- **L89** — Si le lot n'était pas vide…
- **L90** — …valide les offsets de façon asynchrone (sans bloquer la boucle). Une copie de la table est transmise, car l'originale continue d'évoluer.
- **L91** — Fonction de rappel : si la validation a échoué…
- **L92** — …affiche l'erreur. Il n'y a pas de nouvelle tentative : la validation suivante couvrira ces offsets.
- **L93** — Sinon…
- **L94** — …affiche les offsets validés.
- **L95** — Ferme le bloc ouvert ligne 93.
- **L96** — Ferme le bloc ouvert ligne 90.
- **L97** — Ferme le bloc ouvert ligne 89.
- **L98** — Ferme le bloc ouvert ligne 76.

## Lignes 99 à 110 · Arrêt et fermeture

```java
 99          } catch (WakeupException e) {
100              // arrêt demandé
101          } finally {
102              try {
103                  consumer.commitSync(pending);   // dernier commit, synchrone
104              } finally {
105                  consumer.close();
106                  System.out.println("Consumer fermé.");
107              }
108          }
109      }
110  }
```

- **L99** — Intercepte la `WakeupException` de l'arrêt demandé.
- **L100** — Commentaire : rien à faire.
- **L101** — Bloc `finally`, exécuté dans tous les cas (sauf après `halt`).
- **L102** — Ouvre un `try` interne.
- **L103** — Dernière validation, synchrone cette fois : elle est répétée en cas d'erreur passagère et garantit que les derniers messages traités sont bien validés.
- **L104** — `finally` interne : même si cette validation échoue…
- **L105** — …le consommateur est fermé et quitte le groupe.
- **L106** — Confirme la fermeture.
- **L107** — Ferme le bloc ouvert ligne 104.
- **L108** — Ferme le bloc ouvert ligne 101.
- **L109** — Ferme le bloc ouvert ligne 34.
- **L110** — Ferme le bloc ouvert ligne 32.
