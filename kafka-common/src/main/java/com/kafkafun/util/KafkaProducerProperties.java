package com.kafkafun.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerProperties extends AProperties {

    public KafkaProducerProperties() {
        super("producer");
    }

    public KafkaProducerProperties(String propertyFileName) {
        super(propertyFileName);
    }

    @Override
    public Properties getDefaultProperties() {
        Properties properties = new Properties();

        properties.setProperty("topic", Constants.TOPIC);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
