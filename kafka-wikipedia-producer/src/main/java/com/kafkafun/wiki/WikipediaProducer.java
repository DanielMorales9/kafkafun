package com.kafkafun.wiki;

import com.google.gson.JsonElement;
import com.kafkafun.util.ApplicationProperties;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class WikipediaProducer {

    private final String channelName;
    private final String subscribeKey;
    private final String uuid;

    Logger logger = LoggerFactory.getLogger(WikipediaProducer.class.getName());

    public WikipediaProducer() {
        Properties properties = new ApplicationProperties().getProperties();
        channelName = properties.getProperty("channel");
        subscribeKey = properties.getProperty("subscribeKey");
        uuid = UUID.randomUUID().toString();
    }

    public static void main(String[] args) {
        new WikipediaProducer().run();
    }

    public void run() {

        PNConfiguration pnConfiguration = new PNConfiguration();
        pnConfiguration.setSubscribeKey(subscribeKey);
        pnConfiguration.setUuid(uuid);

        PubNub pubnub = new PubNub(pnConfiguration);

        pubnub.addListener(new SubscribeCallback() {

            @Override
            public void status(PubNub pubnub, PNStatus status) {
                logger.info(status.toString());
            }

            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                JsonElement receivedMessageObject = message.getMessage();
                logger.info("Received message: " + receivedMessageObject.toString());
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

        // optionally print the message
        // System.out.println("Message to send: " + messageJsonObject.toString());

        pubnub.subscribe()
                .channels(Collections.singletonList(channelName))
                .execute();
    }
}
