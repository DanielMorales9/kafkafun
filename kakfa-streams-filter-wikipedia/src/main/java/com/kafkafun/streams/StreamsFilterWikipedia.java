package com.kafkafun.streams;

import com.google.gson.JsonParser;
import com.kafkafun.util.ApplicationProperties;
import com.kafkafun.util.KafkaStreamsProperties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterWikipedia {

    public static void main(String[] args) {
        Properties properties = new KafkaStreamsProperties().getProperties();
        Properties appProperties = new ApplicationProperties().getProperties();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream(appProperties.getProperty("in_topic"));
        KStream<String, String> filteredStream = inputTopic.filter((k, wiki) -> {
            try {
                return JsonParser.parseString(wiki)
                        .getAsJsonObject()
                        .get("country")
                        .getAsString().equals("#en.wikipedia");
            } catch (NullPointerException e) {
                return false;
            }
        });

        filteredStream.to(appProperties.getProperty("out_topic"));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }

}
