package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "test-topic";
        String key = "greet";
        String value = "Good morning";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(topicName,key, value);
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(topicName,key+"-1", value+"-2");
        producer.send(record1,(metadata,exception)->{
            if (exception != null) {
                System.out.println("Message sending failed");

            }
            else {
                System.out.println("Message sent successfully");
            }
        });
        producer.close();
        System.out.println("simple producer completed");

    }
}
