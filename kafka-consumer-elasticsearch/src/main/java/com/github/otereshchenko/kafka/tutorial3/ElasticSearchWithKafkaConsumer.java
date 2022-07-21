package com.github.otereshchenko.kafka.tutorial3;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
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

import static com.github.otereshchenko.kafka.core.KafkaProperties.getProperty;
import static org.apache.http.auth.AuthScope.ANY;


public class ElasticSearchWithKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchWithKafkaConsumer.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        String kafkaConsumerGroupId = "my-twitter-tweets-application";
        String kafkaTopic = "twitter-tweets";
        KafkaConsumer<String, String> consumer = KafkaProperties.createConsumer(kafkaConsumerGroupId, kafkaTopic);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));


            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                String id = extractIdFromTweet(json);
                logger.info("json data: " + json);

                IndexRequest request = new IndexRequest("twitter", "tweets", id).source(json, XContentType.JSON);
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);

                String responseId = response.getId();
                logger.info(responseId);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }


            }
        }
//        client.close();
//        consumer.close();
    }

    private static String extractIdFromTweet(String tweet) {
        return JsonParser.parseString(tweet)
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static RestHighLevelClient createClient() {
        CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(ANY, new UsernamePasswordCredentials(getProperty("elastic.search.username"), getProperty("elastic.search.password")));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(getProperty("elastic.search.hostname"), 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider));

        return new RestHighLevelClient(restClientBuilder);
    }
}
