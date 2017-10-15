package com.kafka.custompartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SensorProducer {

    public static void main(String[] args) {

        String topicName = "sensor-topic";

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("speed.sensor.name", "TSS");
        properties.put("partitioner.class","com.kafka.custompartitioner.SensorPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,"SSP" + i, "500" + i));
        }

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,"TSS", "500" + i));
        }

        producer.close();
    }
}
