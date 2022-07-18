package com.github.otereshchenko.kafka.tutorial2;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.ResourceUnauthorizedProblem;
import com.twitter.clientlib.model.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.Collections.emptyList;

public class TwitterProducer {
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    // These secrets should be read from a config file
    private final String bearerToken = "AAAAAAAAAAAAAAAAAAAAAAbcewEAAAAARiLDGD284lp4kNoR2igccAsBeKQ%3DkzYXfE2MPRfrstgMAWw1C01bfODhqoqsaT27o800fxEQKrc0x4";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        List<Tweet> data1 = getData();
        data1.forEach((t) -> logger.info(t.getId() + " " + t.getText()));

        logger.info("End of application");
    }

    public Get2TweetsSearchRecentResponse getResponse() {
        logger.info("Setup");
        String bearerToken = "AAAAAAAAAAAAAAAAAAAAAAbcewEAAAAARiLDGD284lp4kNoR2igccAsBeKQ%3DkzYXfE2MPRfrstgMAWw1C01bfODhqoqsaT27o800fxEQKrc0x4";
        TwitterCredentialsBearer bearer = new TwitterCredentialsBearer(bearerToken);
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
