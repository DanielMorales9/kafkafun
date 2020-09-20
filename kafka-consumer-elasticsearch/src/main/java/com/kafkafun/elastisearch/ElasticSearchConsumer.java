package com.kafkafun.elastisearch;

import com.kafkafun.util.ApplicationProperties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Properties;

public class ElasticSearchConsumer {

    private final String hostname;
    private final String username;
    private final String password;

    public ElasticSearchConsumer() {
        Properties properties = new ApplicationProperties().getProperties();
        this.hostname = properties.getProperty("elasticsearch_hostname");
        this.username = properties.getProperty("elasticsearch_username");
        this.password = properties.getProperty("elasticsearch_password");
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

    public static void main(String[] args) {

    }
}
