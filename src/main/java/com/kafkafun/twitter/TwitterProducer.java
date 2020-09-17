package com.kafkafun.twitter;

import com.google.common.collect.Lists;
import com.kafkafun.util.ApplicationProperties;
import com.kafkafun.util.KafkaProducerProperties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private String topic;
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;

    private KafkaProducer<String, String> producer;
    private final ArrayList<String> terms = Lists.newArrayList("bitcoin");
    private Client client;
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer () {
        Properties prop = new ApplicationProperties().getProperties();
        this.topic = prop.getProperty("topic");
        this.consumerKey = prop.getProperty("consumerKey", "");
        this.consumerSecret = prop.getProperty("consumerSecret", "");
        this.token = prop.getProperty("token", "");
        this.secret = prop.getProperty("secret", "");
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create Producer properties
        Properties properties = new KafkaProducerProperties().getProperties();

        // create the produce
        producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());

        addShutdownHook();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                sendTweetToKafka(msg);
            }
        }
        logger.info("End of Application");
    }

    private void sendTweetToKafka(String msg) {
        producer.send(new ProducerRecord<>(topic, null, msg), (metadata, e) -> {
            if (e != null) {
                logger.error("Something went wrong", e);
            }
        });
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
