package com.danielmorales9.kafkafun;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static final String BOOTSTRAP_SERVER = "0.0.0.0:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the produce
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());

        // create a producer
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", "hello world");

        // send data
        producer.send(record).get();

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }

}
