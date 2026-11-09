package fr.formation.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.ArrayList;
import java.util.List;

/**
 * TP Kafka Streams 4 - transformation 1 -> N : flatMap découpe une phrase en mots.
 * La nouvelle clé est le mot lui-même (flatMap peut changer la clé, contrairement
 * à flatMapValues).
 * Usage : ./run.sh streams.StreamsFlatMap
 */
public class StreamsFlatMap {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> words = builder.<String, String>stream("streams-input")
                .flatMap((key, value) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();
                    if (value != null) {
                        for (String word : value.toLowerCase().split("\\W+")) {
                            if (!word.isBlank()) {
                                result.add(KeyValue.pair(word, key == null ? "" : key));
                            }
                        }
                    }
                    return result;
                });
        words.print(Printed.<String, String>toSysOut().withLabel("mot"));
        words.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-flatmap"));
    }
}
