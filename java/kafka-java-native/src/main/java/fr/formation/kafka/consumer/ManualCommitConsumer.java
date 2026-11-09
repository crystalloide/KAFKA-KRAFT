package fr.formation.kafka.consumer;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * TP03 - Commit MANUEL des offsets ("at least once" maîtrisé).
 * <ul>
 *   <li>enable.auto.commit=false : l'application décide quand l'offset est commité</li>
 *   <li>commitAsync() après chaque lot traité, commitSync() à la fermeture</li>
 *   <li>ConsumerRebalanceListener : commit des offsets avant la perte d'une partition</li>
 * </ul>
 * Un message dont la valeur est "crash" simule une panne AVANT le commit : relancez le
 * programme, les messages non commités sont relus.
 * Usage : ./run.sh consumer.ManualCommitConsumer [topic] [groupe]
 */
public class ManualCommitConsumer {

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "test-formation";
        String group = args.length > 1 ? args[1] : "formation-manual-commit";

        Properties props = KafkaSettings.base("manual-commit-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println(">>> Partitions retirées : " + partitions + " -> commit des offsets traités");
                consumer.commitSync(pending);
                pending.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(">>> Partitions attribuées : " + partitions);
            }
        };

        try {
            consumer.subscribe(List.of(topic), listener);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if ("crash".equalsIgnoreCase(record.value())) {
                        System.out.println("!!! Panne simulée avant le commit de l'offset " + record.offset());
                        Runtime.getRuntime().halt(1);   // arrêt brutal : aucun commit
                    }
                    System.out.printf("traitement partition=%d offset=%d value=%s%n",
                            record.partition(), record.offset(), record.value());
                    // l'offset commité est celui du PROCHAIN message à lire
                    pending.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                if (!records.isEmpty()) {
                    consumer.commitAsync(new HashMap<>(pending), (offsets, ex) -> {
                        if (ex != null) {
                            System.err.println("Echec du commit asynchrone : " + ex.getMessage());
                        } else {
                            System.out.println("offsets commités : " + offsets);
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            // arrêt demandé
        } finally {
            try {
                consumer.commitSync(pending);   // dernier commit, synchrone
            } finally {
                consumer.close();
                System.out.println("Consumer fermé.");
            }
        }
    }
}
