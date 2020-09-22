package com.kafkafun.elastisearch;

import com.kafkafun.util.ApplicationProperties;
import com.kafkafun.util.KafkaConsumerProperties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private final String hostname;
    private final String username;
    private final String password;
    private final String topic;
    private final String index;

    public ElasticSearchConsumer() {
        Properties properties = new ApplicationProperties().getProperties();
        this.topic = properties.getProperty("topic");
        this.hostname = properties.getProperty("elasticsearch_hostname");
        this.username = properties.getProperty("elasticsearch_username");
        this.password = properties.getProperty("elasticsearch_password");
        this.index = properties.getProperty("elasticsearch_index");
    }

    public RestHighLevelClient createClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        ElasticSearchConsumer elasticSearch = new ElasticSearchConsumer();
        RestHighLevelClient client = elasticSearch.createClient();

        KafkaConsumer<String, String> consumer = elasticSearch.createConsumer();

        consumer.subscribe(Collections.singleton(elasticSearch.topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {

                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                IndexRequest indexRequest = new IndexRequest(elasticSearch.index)
                        .id(id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                logger.info(indexResponse.status().toString());
                logger.info(indexResponse.getId());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        // client.close();
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new KafkaConsumerProperties().getProperties();
        return new KafkaConsumer<>(properties);
    }
}
