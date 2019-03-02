package com.github.jpalmer1026.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String value = "hello world " + i;
            String key = "id_ " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(FIRST_TOPIC, key, value);

            logger.info("Key: " + key);

            // send data - asynchronous
            producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); //block the send to make it synchronous - don't do this in production
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
