package fr.formation.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Set;

/**
 * Partitionneur personnalisé : les clients VIP (C-001, C-002) sont isolés dans
 * la partition 0 ; les autres clés sont réparties (murmur2) sur les partitions 1..n-1.
 * Activé par : partitioner.class=fr.formation.kafka.producer.VipPartitioner
 */
public class VipPartitioner implements Partitioner {

    private static final Set<String> VIP = Set.of("C-001", "C-002");

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int partitions = cluster.partitionsForTopic(topic).size();
        if (key == null || partitions < 2) {
            return 0;
        }
        if (VIP.contains(key.toString())) {
            return 0;
        }
        // même fonction de hachage que le partitionneur par défaut de Kafka
        return 1 + Utils.toPositive(Utils.murmur2(keyBytes)) % (partitions - 1);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // pas de configuration
    }

    @Override
    public void close() {
        // rien à libérer
    }
}
