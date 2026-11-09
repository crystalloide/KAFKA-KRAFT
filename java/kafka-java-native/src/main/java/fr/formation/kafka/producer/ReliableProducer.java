package fr.formation.kafka.producer;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalTime;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TP03 - Producer "production" : garanties de livraison.
 * <ul>
 *   <li>acks=all + min.insync.replicas=2 (topic) : écriture confirmée par 2 réplicas minimum</li>
 *   <li>enable.idempotence=true : pas de doublon ni de désordre en cas de retry</li>
 *   <li>retries + delivery.timeout.ms : on réessaie pendant 2 minutes maximum</li>
 *   <li>linger.ms / batch.size / compression : débit (voir TP05)</li>
 * </ul>
 * Envoie 1 message par seconde jusqu'à Ctrl+C : arrêtez des brokers pendant
 * l'exécution pour observer le comportement (voir TP02 et TP03).
 * Usage : ./run.sh producer.ReliableProducer [topic] [acks]
 */
public class ReliableProducer {

    public static void main(String[] args) throws InterruptedException {
        String topic = args.length > 0 ? args[0] : "test-formation";
        String acks = args.length > 1 ? args[1] : "all";

        Properties props = KafkaSettings.base("reliable-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // --- Garanties de livraison ---
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, acks.equals("all"));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        // --- Débit ---
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong ok = new AtomicLong();
        AtomicLong ko = new AtomicLong();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        System.out.printf("Envoi vers '%s' avec acks=%s (Ctrl+C pour arrêter)%n", topic, acks);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long i = 0;
            while (running.get()) {
                final long n = i++;
                String value = "message " + n + " envoyé à " + LocalTime.now();
                producer.send(new ProducerRecord<>(topic, "cle-" + (n % 6), value), (md, ex) -> {
                    if (ex == null) {
                        ok.incrementAndGet();
                        System.out.printf("[OK] %-45s partition=%d offset=%d%n", value, md.partition(), md.offset());
                    } else {
                        ko.incrementAndGet();
                        System.err.printf("[KO] message %d : %s - %s%n", n,
                                ex.getClass().getSimpleName(), ex.getMessage());
                    }
                });
                Thread.sleep(1000);
            }
        }
        System.out.printf("Fin : %d acquittés, %d en échec%n", ok.get(), ko.get());
    }
}
