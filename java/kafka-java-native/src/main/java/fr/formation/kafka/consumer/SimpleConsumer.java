package fr.formation.kafka.consumer;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * TP03 - Consumer au sein d'un consumer group, commit automatique des offsets.
 * <p>
 * Lancez plusieurs instances avec le MEME groupe : les partitions du topic sont
 * réparties entre elles (consommation en parallèle). Avec un groupe DIFFERENT,
 * chaque instance reçoit tous les messages (publish / subscribe).
 * Usage : ./run.sh consumer.SimpleConsumer [topic] [groupe]
 */
public class SimpleConsumer {

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "test-formation";
        String group = args.length > 1 ? args[1] : "formation-group";

        Properties props = KafkaSettings.base("simple-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        // Un consumer DESERIALISE la clé et la valeur
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Si le groupe n'a encore aucun offset commité : lecture depuis le début
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Commit automatique toutes les secondes (sémantique "au moins une fois" approximative)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // Arrêt propre sur Ctrl+C : wakeup() fait lever une WakeupException dans poll()
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(topic));
            System.out.printf("Consumer du groupe '%s' abonné à '%s' (Ctrl+C pour arrêter)%n", group, topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            // arrêt demandé
        } finally {
            consumer.close();   // quitte le groupe proprement : rééquilibrage immédiat
            System.out.println("Consumer fermé.");
        }
    }
}
