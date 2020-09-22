package com.kafkafun.wiki;

import com.google.gson.JsonElement;
import com.kafkafun.util.ApplicationProperties;
import com.kafkafun.util.KafkaProducerProperties;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.objects_api.channel.PNChannelMetadataResult;
import com.pubnub.api.models.consumer.objects_api.membership.PNMembershipResult;
import com.pubnub.api.models.consumer.objects_api.uuid.PNUUIDMetadataResult;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import com.pubnub.api.models.consumer.pubsub.PNSignalResult;
import com.pubnub.api.models.consumer.pubsub.files.PNFileEventResult;
import com.pubnub.api.models.consumer.pubsub.message_actions.PNMessageActionResult;
import com.sun.istack.internal.NotNull;
import com.sun.xml.internal.ws.api.message.ExceptionHasMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class WikipediaProducer {

    Logger logger = LoggerFactory.getLogger(WikipediaProducer.class.getName());
    private final String channelName;
    private final PubNub pubnub;


    private final String topic;

    private final KafkaProducer<String, String> producer;

    public WikipediaProducer() {
        Properties properties = new ApplicationProperties().getProperties();
        channelName = properties.getProperty("channel");
        topic = properties.getProperty("topic");

        String subscribeKey = properties.getProperty("subscribeKey");
        String uuid = UUID.randomUUID().toString();

        // create PubNubClient
        pubnub = getPubNub(subscribeKey, uuid);

        // create Producer properties
        Properties kafkaProperties = new KafkaProducerProperties().getProperties();

        // create the produce
        producer = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
    }

    private PubNub getPubNub(String subscribeKey, String uuid) {
        System.out.println(subscribeKey);

        PNConfiguration pnConfiguration = new PNConfiguration();
        pnConfiguration.setSubscribeKey(subscribeKey);
        pnConfiguration.setUuid(uuid);

        return new PubNub(pnConfiguration);
    }

    public static void main(String[] args) {
        new WikipediaProducer().run();
    }

    public void run() {
        addShutdownHook();

        pubnub.addListener(new SubscribeCallback() {

            @Override
            public void status(PubNub pubnub, PNStatus status) {
                logger.info(status.toString());
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                JsonElement receivedMessageObject = message.getMessage();
                String msg = receivedMessageObject.toString();
                logger.info("Received message: " + msg);
                sendTweetToKafka(msg);
            }

            @Override
            public void presence(PubNub pubNub, PNPresenceEventResult pnPresenceEventResult) {

            }

            @Override
            public void signal(PubNub pubNub, PNSignalResult pnSignalResult) {

            }

            @Override
            public void uuid(PubNub pubNub, PNUUIDMetadataResult pnuuidMetadataResult) {

            }

            @Override
            public void channel(PubNub pubNub, PNChannelMetadataResult pnChannelMetadataResult) {

            }

            @Override
            public void membership(PubNub pubNub, PNMembershipResult pnMembershipResult) {

            }

            @Override
            public void messageAction(PubNub pubNub, PNMessageActionResult pnMessageActionResult) {

            }

            @Override
            public void file(PubNub pubNub, PNFileEventResult pnFileEventResult) {

            }

        });

        pubnub.subscribe()
                .channels(Collections.singletonList(channelName))
                .execute();
    }

    private void sendTweetToKafka(String msg) {
        producer.send(new ProducerRecord<>(this.topic, null, msg), (metadata, e) -> {
            if (e != null) {
                logger.error("Something went wrong", e);
            }
        });
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private void close() {
        logger.info("Unsubscribing...");
        pubnub.unsubscribe();
        logger.info("Stopping producer...");
        producer.close();
        logger.info("done!");
    }
}
