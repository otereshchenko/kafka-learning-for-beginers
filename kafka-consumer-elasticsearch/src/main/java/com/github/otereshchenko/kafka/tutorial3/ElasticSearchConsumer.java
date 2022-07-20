package com.github.otereshchenko.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

import static com.github.otereshchenko.kafka.tutorial3.ElasticSearchProperties.*;
import static org.apache.http.auth.AuthScope.ANY;


public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        String json = "{\"foo\":\"bar\"}";
        IndexRequest request = new IndexRequest("twitter", "tweets").source(json, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String id = response.getId();
        logger.info(id);
        client.close();
    }

    private static RestHighLevelClient createClient() {
        CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(ANY, new UsernamePasswordCredentials(ELASTIC_SEARCH_USERNAME, ELASTIC_SEARCH_PASSWORD));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        new HttpHost(ELASTIC_SEARCH_HOSTNAME, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider));

        return new RestHighLevelClient(restClientBuilder);
    }
}
