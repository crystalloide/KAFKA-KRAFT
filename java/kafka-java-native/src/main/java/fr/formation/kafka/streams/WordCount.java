package fr.formation.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

/**
 * TP Kafka Streams 6 - traitement AVEC ETAT : comptage de mots (KTable).
 * <ul>
 *   <li>flatMapValues + groupBy : repartitionnement par mot (topic interne -repartition)</li>
 *   <li>count() : état stocké dans RocksDB + topic interne -changelog (tolérance aux pannes)</li>
 *   <li>processing.guarantee=exactly_once_v2</li>
 * </ul>
 * Usage : ./run.sh streams.WordCount
 */
public class WordCount {

    public static void main(String[] args) {
        Properties props = StreamsSupport.config("wordcount-app");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Long> counts = builder.<String, String>stream("words-input")
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                .filter((key, word) -> !word.isBlank())
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        counts.toStream()
                .peek((word, count) -> System.out.printf("%-15s %d%n", word, count))
                .to("words-output", Produced.with(Serdes.String(), Serdes.Long()));

        StreamsSupport.run(builder, props);
    }
}
