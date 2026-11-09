package fr.formation.kafka.transactions;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * TP03 - Traitement "exactly-once" : consume -> transform -> produce.
 * <p>
 * Les offsets consommés sont commités DANS la transaction du producer
 * (sendOffsetsToTransaction) : lecture, écriture et commit sont atomiques.
 * Le consumer lit en isolation.level=read_committed : il ignore les messages
 * des transactions annulées.
 * Usage : ./run.sh transactions.ExactlyOnceProcessor
 */
public class ExactlyOnceProcessor {

    public static void main(String[] args) {
        Properties cprops = KafkaSettings.base("eos-consumer");
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-processor");
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Properties pprops = KafkaSettings.base("eos-producer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "formation-eos-processor-1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);
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
            producer.initTransactions();
            consumer.subscribe(List.of("transactions-in"));
            System.out.println("transactions-in --(majuscules)--> transactions-out  (Ctrl+C pour arrêter)");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    continue;
                }
                producer.beginTransaction();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, String> r : records) {
                        String transformed = r.value().toUpperCase();
                        producer.send(new ProducerRecord<>("transactions-out", r.key(), transformed));
                        offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                        System.out.printf("lu [%s] -> écrit [%s]%n", r.value(), transformed);
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                    System.out.println("  transaction commitée (" + records.count() + " messages)");
                } catch (KafkaException e) {
                    producer.abortTransaction();
                    System.err.println("  transaction annulée : " + e.getMessage());
                }
            }
        } catch (WakeupException e) {
            // arrêt demandé
        } finally {
            producer.close();
            consumer.close();
            System.out.println("Processeur arrêté.");
        }
    }
}
