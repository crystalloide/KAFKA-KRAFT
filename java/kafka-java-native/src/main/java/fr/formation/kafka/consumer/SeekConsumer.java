package fr.formation.kafka.consumer;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TP03 - Relecture : assign() manuel (sans consumer group) et seek() par date.
 * Relit tous les messages publiés depuis N minutes, puis s'arrête.
 * Usage : ./run.sh consumer.SeekConsumer [topic] [minutes]
 */
public class SeekConsumer {

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "test-formation";
        long minutes = args.length > 1 ? Long.parseLong(args[1]) : 10;

        Properties props = KafkaSettings.base("seek-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map((PartitionInfo p) -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(partitions);   // pas de group.id : aucun rééquilibrage, aucun commit

            long since = Instant.now().minus(Duration.ofMinutes(minutes)).toEpochMilli();
            Map<TopicPartition, Long> query = partitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> since));
            Map<TopicPartition, OffsetAndTimestamp> found = consumer.offsetsForTimes(query);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            for (TopicPartition tp : partitions) {
                OffsetAndTimestamp oat = found.get(tp);
                long start = (oat != null) ? oat.offset() : endOffsets.get(tp);
                consumer.seek(tp, start);
                System.out.printf("%s : relecture à partir de l'offset %d (fin = %d)%n", tp, start, endOffsets.get(tp));
            }

            int empty = 0;
            while (empty < 3) {
                var records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    empty++;
                }
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("%s partition=%d offset=%d value=%s%n",
                            Instant.ofEpochMilli(r.timestamp()), r.partition(), r.offset(), r.value());
                }
            }
        }
    }
}
