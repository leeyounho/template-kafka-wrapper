package com.younho;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new AnnotationConfigApplicationContext(MetricsConfig.class, KafkaConfig.class);

        KafkaConfig kafkaConfig = context.getBean(KafkaConfig.class);

        String bootstrapServers = "localhost:9092";
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        Map<String, Object> replyConsumerProps = new HashMap<>();
        replyConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        replyConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        replyConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        replyConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group" + "-reply"); // TODO
        replyConsumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        replyConsumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        kafkaConfig.setProducerProps(producerProps);
        kafkaConfig.setConsumerProps(consumerProps);
        kafkaConfig.setReplyConsumerProps(replyConsumerProps);
        kafkaConfig.setMySubject("TEST-TOPIC");
        kafkaConfig.setDestSubject("TEST-TOPIC-2");
        KafkaWrapper<KafkaMsg> kafkaWrapper = kafkaConfig.createInstance();
        kafkaWrapper.init();

        ReplyKafkaWrapper replyKafkaWrapper = new ReplyKafkaWrapper("localhost:9092", "TEST-TOPIC-2", "TEST-TOPIC");
        replyKafkaWrapper.init();

        Thread.sleep(5000);

        KafkaMsg kafkaMsg = new KafkaMsg();
        kafkaMsg.setStringField("TEST_A_FIELD");
        kafkaMsg.setObjectField(null);
        kafkaMsg.setBooleanField(true);
        kafkaMsg.setByteArrayField("TEST_BYTE_ARRAY".getBytes());

//        try {
//            kafkaWrapper.send(kafkaMsg);
//        } catch (Exception e) {
//        }
//        Thread.sleep(3000);

        try {
            kafkaWrapper.sendKafkaRequest(kafkaMsg);
        } catch (Exception e) {
        }
    }
}