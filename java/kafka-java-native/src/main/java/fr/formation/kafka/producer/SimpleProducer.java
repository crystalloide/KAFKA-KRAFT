package fr.formation.kafka.producer;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * TP03 - Premier producer : les 3 façons d'envoyer un message.
 * <ol>
 *   <li>"fire and forget" : send() sans attendre de réponse</li>
 *   <li>synchrone        : send().get() bloque jusqu'à l'acquittement du broker</li>
 *   <li>asynchrone       : send(record, callback) - recommandé en production</li>
 * </ol>
 * Usage : ./run.sh producer.SimpleProducer [topic] [nombre]
 */
public class SimpleProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = args.length > 0 ? args[0] : "test-formation";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        Properties props = KafkaSettings.base("simple-producer");
        // Un producer SERIALISE la clé et la valeur en tableaux d'octets
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Valeur par défaut depuis Kafka 3.0 : acquittement par tous les réplicas in-sync
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // try-with-resources : close() vide le buffer (flush) puis libère les connexions
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            System.out.println("=== 1. Fire and forget ===");
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "cle-" + (i % 3), "fire-and-forget " + i));
            }
            producer.flush();

            System.out.println("=== 2. Envoi synchrone (send().get()) ===");
            for (int i = 0; i < count; i++) {
                RecordMetadata md = producer.send(
                        new ProducerRecord<>(topic, "cle-" + (i % 3), "synchrone " + i)).get();
                System.out.printf("synchrone %d -> partition %d, offset %d%n", i, md.partition(), md.offset());
            }

            System.out.println("=== 3. Envoi asynchrone avec callback ===");
            for (int i = 0; i < count; i++) {
                final int n = i;
                producer.send(new ProducerRecord<>(topic, "cle-" + (i % 3), "asynchrone " + i),
                        (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("ECHEC message %d : %s%n", n, exception.getMessage());
                            } else {
                                System.out.printf("asynchrone %d -> partition %d, offset %d%n",
                                        n, metadata.partition(), metadata.offset());
                            }
                        });
            }
        }
        System.out.println("Producer fermé proprement.");
    }
}
