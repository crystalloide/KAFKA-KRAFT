package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

/**
 * TP Kafka Streams 2 - filtrage : ne conserve que les messages contenant "error".
 * Usage : ./run.sh streams.StreamsFilter
 */
public class StreamsFilter {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-input");
        KStream<String, String> errors = source
                .filter((key, value) -> value != null && value.toLowerCase().contains("error"));
        errors.print(Printed.<String, String>toSysOut().withLabel("erreur"));
        errors.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-filter"));
    }
}
