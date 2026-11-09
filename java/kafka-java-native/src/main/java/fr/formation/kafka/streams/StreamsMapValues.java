package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

/**
 * TP Kafka Streams 3 - transformation 1 -> 1 : mapValues (la clé n'est pas modifiée,
 * donc pas de repartitionnement).
 * Usage : ./run.sh streams.StreamsMapValues
 */
public class StreamsMapValues {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> upper = builder.<String, String>stream("streams-input")
                .mapValues(value -> value == null ? null : value.toUpperCase());
        upper.print(Printed.<String, String>toSysOut().withLabel("majuscules"));
        upper.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-mapvalues"));
    }
}
