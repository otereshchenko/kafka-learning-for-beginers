package com.github.otereshchenko.kafka.tutorial2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Objects;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.github.otereshchenko.kafka.core.KafkaProperties.PRODUCER_PROPS;
import static com.github.otereshchenko.kafka.core.KafkaProperties.getProperty;
import static java.util.Collections.emptyList;

public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private TwitterProducer() {
        mapper.setSerializationInclusion(NON_NULL);
    }

    private void run() {
        KafkaProducer<String, String> producer = getKafkaProducer();
        Callback sendingCallback = (metadata, ex) -> {
            if (ex != null) {
                logger.error("Something bad happened", ex);
            }
        };

        List<Tweet> data = getTweets();
        data.stream().map(this::toJson)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .peek(logger::info)
                .map(this::getRecord)
                .forEach(r -> producer.send(r, sendingCallback));

        logger.info("End of application");
    }

    private Optional<String> toJson(Tweet tweet) {
        try {
            return Optional.of(mapper.writeValueAsString(tweet));
        } catch (JsonProcessingException e) {
            logger.error("Exception during converting to JSON", e);
            return Optional.empty();
        }
    }

    private ProducerRecord<String, String> getRecord(String message) {
        return new ProducerRecord<>(getProperty("kafka.topic.twitter.tweets"), null, message);
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
        TwitterCredentialsBearer bearer = new TwitterCredentialsBearer(getProperty("twitter.bearer.token"));
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

    public List<Tweet> getTweets() {
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
