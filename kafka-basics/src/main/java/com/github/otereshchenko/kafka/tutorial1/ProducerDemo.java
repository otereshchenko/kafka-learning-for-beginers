package com.github.otereshchenko.kafka.tutorial1;

import com.github.otereshchenko.kafka.core.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
    public static void main(String[] args) {
        String topic = "first-topic";
        String message = "hello new world";

        //create  record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProperties.PRODUCER_PROPS);
        //send data -async
        producer.send(record);

        producer.flush();
        producer.close();


    }
}
