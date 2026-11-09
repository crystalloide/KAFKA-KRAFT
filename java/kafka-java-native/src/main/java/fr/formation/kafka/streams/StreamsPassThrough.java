package fr.formation.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * TP Kafka Streams 1 - copie simple d'un topic vers un autre (sans transformation).
 * Usage : ./run.sh streams.StreamsPassThrough
 */
public class StreamsPassThrough {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-input");
        source.print(Printed.<String, String>toSysOut().withLabel("lu"));
        source.to("streams-output", Produced.with(Serdes.String(), Serdes.String()));
        StreamsSupport.run(builder, StreamsSupport.config("streams-passthrough"));
    }
}
