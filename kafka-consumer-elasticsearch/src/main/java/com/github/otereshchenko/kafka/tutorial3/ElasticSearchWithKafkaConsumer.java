package com.github.otereshchenko.kafka.tutorial3;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

import static com.github.otereshchenko.kafka.core.KafkaProperties.getProperty;
import static org.apache.http.auth.AuthScope.ANY;


public class ElasticSearchWithKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchWithKafkaConsumer.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        String kafkaConsumerGroupId = "my-twitter-tweets-application";
        String kafkaTopic = "twitter-tweets";
        KafkaConsumer<String, String> consumer = createConsumer(kafkaConsumerGroupId, kafkaTopic);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int countRecords = records.count();
            logger.info("Received " + countRecords + " records.");

            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                String id = extractIdFromTweet(json);
                logger.info("json data: " + json);

                IndexRequest request = new IndexRequest("twitter", "tweets", id).source(json, XContentType.JSON);
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);

                String responseId = response.getId();
                logger.info(responseId);

                doSleep(10);


            }
            if (countRecords > 0) {
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                doSleep(1000);
            }
        }
//        client.close();
//        consumer.close();
    }

    private static void doSleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static KafkaConsumer<String, String> createConsumer(String groupId, String topic) {
        Properties consumeProps = KafkaProperties.CONSUMER_BASIC_PROPS;
        consumeProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumeProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//disable autocommit of offsets
        consumeProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
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
