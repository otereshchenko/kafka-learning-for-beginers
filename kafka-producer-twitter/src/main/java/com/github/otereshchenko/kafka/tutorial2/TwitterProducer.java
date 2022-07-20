package com.github.otereshchenko.kafka.tutorial2;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.ResourceUnauthorizedProblem;
import com.twitter.clientlib.model.Tweet;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.otereshchenko.kafka.core.KafkaProperties.PRODUCER_PROPS;
import static com.github.otereshchenko.kafka.tutorial2.TwitterProperties.KAFKA_TOPIC;
import static com.github.otereshchenko.kafka.tutorial2.TwitterProperties.TWITTER_BEARER_TOKEN;
import static java.util.Collections.emptyList;

public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        KafkaProducer<String, String> producer = getKafkaProducer();
        Callback sendingCallback = (metadata, ex) -> {
            if (ex != null) {
                logger.error("Something bad happened", ex);
            }
        };

        List<Tweet> data = getData();


        data.stream().map(t -> "Message ID: " + t.getId() + ",  Message text: " + t.getText())
                .peek(logger::info)
                .map(this::getRecord)
                .forEach(r -> producer.send(r, sendingCallback));

        logger.info("End of application");
    }

    private ProducerRecord<String, String> getRecord(String message) {
        return new ProducerRecord<>(KAFKA_TOPIC, null, message);
    }

    private KafkaProducer<String, String> getKafkaProducer() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_PROPS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("sutting down client from twitter...");
//            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        return producer;
    }

    public Get2TweetsSearchRecentResponse getResponse() {
        logger.info("Setup");
        TwitterCredentialsBearer bearer = new TwitterCredentialsBearer(TWITTER_BEARER_TOKEN);
        TwitterApi apiInstance = new TwitterApi(bearer);

        try {
            return apiInstance.tweets()
                    .tweetsRecentSearch("bitcoin")
                    .maxResults(20)
                    .execute();
        } catch (ApiException e) {
            logger.error("Polling message failed. ", e);
            return null;
        }
    }

    public List<Tweet> getData() {
        Get2TweetsSearchRecentResponse result = getResponse();
        if (result == null) {
            return emptyList();
        }
        if (result.getErrors() != null && result.getErrors().size() > 0) {
            logger.error("Error:");
            result.getErrors().forEach(e -> {
                logger.error(e.toString());
                if (e instanceof ResourceUnauthorizedProblem) {
                    logger.error(e.getTitle() + " " + e.getDetail());
                }
            });
            return emptyList();
        }
        List<Tweet> data = result.getData();
        return data == null ? emptyList() : result.getData();
    }
}
