package com.kafkafun;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        // create Producer properties
        Properties properties = KafkaProperties.getProducerProperties();

        // create the produce
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());

        for (int i = 0; i < 10; i++) {

            // create a producer
            String message = String.format("hello world %d", i);
            String key = String.format("id_%d", i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(Constants.TOPIC, key, message);

            logger.info("Key: " + key);

            // send data
            producer.send(record, (recordMetadata, e) -> {
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
            });

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }

}
