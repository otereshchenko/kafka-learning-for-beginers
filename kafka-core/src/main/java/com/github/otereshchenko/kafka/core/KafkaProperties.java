package com.github.otereshchenko.kafka.core;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class KafkaProperties {
    private static Properties kafkaProperties;

    static {
        try (InputStream input = new FileInputStream("kafka-core/src/main/resources/kafka-config.properties")) {
            kafkaProperties = new Properties();
            // load a properties file
            kafkaProperties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return kafkaProperties.getProperty(key, "");
    }

    public static final Properties PRODUCER_PROPS = new Properties() {{
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("kafka.bootstrap.servers"));
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }};

    public static final Properties CONSUMER_BASIC_PROPS = new Properties() {{
        setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("kafka.bootstrap.servers"));
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }};

    public static KafkaConsumer<String, String> createConsumer(String groupId, String topic) {
        Properties consumeProps = KafkaProperties.CONSUMER_BASIC_PROPS;
        consumeProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumeProps);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
