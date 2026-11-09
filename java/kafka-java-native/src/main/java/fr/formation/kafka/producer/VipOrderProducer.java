package fr.formation.kafka.producer;

import fr.formation.kafka.common.KafkaSettings;
import fr.formation.kafka.common.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TP03 - Utilisation d'un partitionneur personnalisé (VipPartitioner).
 * Usage : ./run.sh producer.VipOrderProducer [topic]
 */
public class VipOrderProducer {

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "orders";

        Properties props = KafkaSettings.base("vip-order-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VipPartitioner.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (Order order : Order.loadCsv(KafkaSettings.dataFile("orders.csv"))) {
                producer.send(new ProducerRecord<>(topic, order.customerId(), order.toJson()), (md, ex) -> {
                    if (ex == null) {
                        System.out.printf("%s client=%s -> partition %d%n",
                                order.orderId(), order.customerId(), md.partition());
                    }
                });
            }
        }
    }
}
