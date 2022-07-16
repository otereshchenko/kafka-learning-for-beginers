package com.github.otereshchenko.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {
        String topic = "first-topic";

        Properties consumeProps = KafkaProperties.CONSUMER_BASIC_PROPS;

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps);
        //assign and seek are mostly used to reply data or fetch a specific message
        //assign
        long offsetToReadFrom = 15L;
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(topicPartitionToReadFrom));
        //seek
        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        //poll new data
        int numberOfMessages = 5;
        boolean keepOnReading = true;
        int numberOfMessagesToReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesToReadSoFar++;
                logger.info("Received metadata. \n" +
                        "\n Key: " + record.key() +
                        "\n Value: " + record.value() +
                        "\n Partition: " + record.partition() +
                        "\n Offset: " + record.offset() +
                        "\n Timestamp: " + record.timestamp() +
                        "\n"
                );

                if (numberOfMessagesToReadSoFar >= numberOfMessages){
                    keepOnReading = false;//exit while loop
                    break;//exit for loop
                }
            }
        }
        logger.info("Exit the application");
    }
}
