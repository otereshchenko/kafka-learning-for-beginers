package com.github.otereshchenko.kafka.tutorial1;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private ConsumerDemoWithThread() {

    }

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        String topic = "first-topic";
        String groupId = "my-sixth-application";

        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumer = new ConsumerThread(topic, groupId, latch);

        Thread thread = new Thread(myConsumer);
        thread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            tryAwait(latch);
        }));

        tryAwait(latch);
    }

    private void tryAwait(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
        } finally {
            logger.info("Application is clothing");
        }
    }

    public class ConsumerThread implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerThread(String topic, String groupId, CountDownLatch latch) {
            this.latch = latch;

            Properties consumeProps = KafkaProperties.CONSUMER_BASIC_PROPS;
            consumeProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            this.consumer = new KafkaConsumer<>(consumeProps);
            consumer.subscribe(Collections.singletonList(topic));

        }

        @Override
        public void run() {
            try {
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
            } catch (WakeupException we) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                //tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //may throw WakeupException
            consumer.wakeup();
        }
    }
}
