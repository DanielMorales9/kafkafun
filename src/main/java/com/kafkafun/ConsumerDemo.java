package com.kafkafun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {

    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public static final String GROUP_ID = "my_application";

    public static void main(String[] args) {

        new ConsumerDemo().run();

    }

    public void run() {

        Properties properties = KafkaProperties.getConsumerProperties(GROUP_ID);
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerThread target = new ConsumerThread(latch, properties);
        new Thread(target).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shudown hook");
            target.shutdown();
            awaitLatch(latch);
        }));

        awaitLatch(latch);

    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is exiting");
        }
    }

    public class ConsumerThread implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;


        public ConsumerThread(CountDownLatch latch, Properties properties) {
            this.latch = latch;
            this.consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            consumer.subscribe(Collections.singletonList(Constants.TOPIC));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
