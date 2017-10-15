package com.kafka.custompartitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class SensorPartitioner implements Partitioner {

    private String speedSensorName;

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int sp = (int) (numPartitions * 0.3);
        int p = 0;
        if ((keyBytes == null) || !(key instanceof String)) {
            throw new InvalidRecordException("All messages must have sensor name as key");
        }

        if (key.equals(speedSensorName))
            p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
        else
            p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;

        System.out.println("Key : " + key + " Partition :" + p);
        return p;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();

    }
}
