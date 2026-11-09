package fr.formation.kafka.producer;

import fr.formation.kafka.common.KafkaSettings;
import fr.formation.kafka.common.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TP03 - Partitionnement par clé et ordre des messages.
 * <p>
 * Envoie le jeu de données data/orders.csv au format JSON dans le topic "orders"
 * avec customerId comme CLE : toutes les commandes d'un même client arrivent
 * dans la même partition et sont donc lues dans l'ordre d'envoi.
 * Usage : ./run.sh producer.OrderProducer [topic] [pause_ms]
 */
public class OrderProducer {

    public static void main(String[] args) throws InterruptedException {
        String topic = args.length > 0 ? args[0] : "orders";
        long pauseMs = args.length > 1 ? Long.parseLong(args[1]) : 0L;

        Properties props = KafkaSettings.base("order-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        List<Order> orders = Order.loadCsv(KafkaSettings.dataFile("orders.csv"));
        Map<String, Integer> partitionByCustomer = new ConcurrentHashMap<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (Order order : orders) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, order.customerId(), order.toJson());
                producer.send(record, (md, ex) -> {
                    if (ex != null) {
                        System.err.println("ECHEC " + order.orderId() + " : " + ex.getMessage());
                        return;
                    }
                    partitionByCustomer.put(order.customerId(), md.partition());
                    System.out.printf("%s client=%s -> partition %d offset %d%n",
                            order.orderId(), order.customerId(), md.partition(), md.offset());
                });
                if (pauseMs > 0) {
                    Thread.sleep(pauseMs);
                }
            }
        }
        System.out.println("\n=== Partition retenue pour chaque clé (customerId) ===");
        new TreeMap<>(partitionByCustomer).forEach((customer, partition) ->
                System.out.printf("  %s -> partition %d%n", customer, partition));
        System.out.printf("%d commandes envoyées dans '%s'.%n", orders.size(), topic);
    }
}
