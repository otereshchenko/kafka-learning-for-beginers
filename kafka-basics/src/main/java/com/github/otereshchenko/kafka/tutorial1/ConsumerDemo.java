package com.github.otereshchenko.kafka.tutorial1;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        String topic = "first-topic";
        String groupId = "my-fourth-application";

        Properties consumeProps = KafkaProperties.CONSUMER_BASIC_PROPS;
        consumeProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received metadata. \n" +
                            "\n Key: " + record.key() +
                            "\n Value: " + record.value() +
                            "\n Partition: " + record.partition() +
                            "\n Offset: " + record.offset() +
                            "\n Timestamp: " + record.timestamp() +
                            "\n"
                    );
                }
            }
        }
    }
}
