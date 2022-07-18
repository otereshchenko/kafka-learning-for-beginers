package com.github.otereshchenko.kafka.tutorial1;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProducerWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy 'at' hh:mm:ss");

    public static void main(String[] args) {
        String topic = "first-topic";
        String currentTime = timeFormatter.format(LocalDateTime.now());

        logger.info("Kafka produce messages since: " + currentTime);

        for (int i = 1; i < 6; i++) {
            String message = currentTime + " hello world " + i;
            String key = "key_id_" + i;

            logger.info(key);
            //create  record
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            submit(record);
        }
    }

    private static void submit(final ProducerRecord<String, String> record) {
        //create producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaProperties.PRODUCER_PROPS)) {
            //send data -async
            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    logger.error(exception.getMessage());
                }
                logger.info("Received metadata. \n" +
                        "\n Topic: " + metadata.topic() +
                        "\n Partition: " + metadata.partition() +
                        "\n Offset: " + metadata.offset() +
                        "\n Timestamp: " + metadata.timestamp() +
                        "\n"
                );
            };
            producer.send(record, callback)
//                    .get() //make it sync
            ;
        }
    }
}
