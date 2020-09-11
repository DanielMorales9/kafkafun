package com.danielmorales9.kafkafun;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static final String BOOTSTRAP_SERVER = "0.0.0.0:9092";
    public static final String MY_TOPIC = "first";

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the produce
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());

        for (int i = 0; i < 10; i++) {

            // create a producer
            String message = String.format("hello world %d", i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(MY_TOPIC, message);

            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata: \n" +
                                "Topic: " +  recordMetadata.topic() + "\n" +
                                "Partition: " +  recordMetadata.partition() + "\n" +
                                "Offset: " +  recordMetadata.offset() + "\n" +
                                "Timestamp: " +  recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Error while producing", e);
                    }
                }
            });

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }

}
