package com.kafkafun.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerProperties extends AProperties {

    public KafkaConsumerProperties() {
        super("consumer");
    }

    public KafkaConsumerProperties(String propertyFileName) {
        super(propertyFileName);
    }

    @Override
    public Properties getDefaultProperties() {
        Properties properties = new Properties();

        properties.setProperty("topic", Constants.TOPIC);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_CONFIG_EARLIEST);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.CONSUMER_GROUP_ID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }

}
