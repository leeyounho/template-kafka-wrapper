package com.younho;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

public class ReplyKafkaWrapper implements MessageListener<String, KafkaMsg> {
    private static Logger logger = LoggerFactory.getLogger(ReplyKafkaWrapper.class);

    private ProducerFactory<String, KafkaMsg> producerFactory;
    private ConsumerFactory<String, KafkaMsg> consumerFactory;
    private KafkaMessageListenerContainer<String, KafkaMsg> container;
    private KafkaTemplate<String, KafkaMsg> kafkaTemplate;
    private String bootStrapServers;
    private String mySubject;
    private String destSubject;

    public ReplyKafkaWrapper(String bootStrapServers, String mySubject, String destSubject) {
        this.bootStrapServers = bootStrapServers;
        this.mySubject = mySubject;
        this.destSubject = destSubject;
    }

    public void init() {
        // producer factory
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMsgJsonSerializer.class);
        this.producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        // consumer factory
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaMsgJsonDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "consumer-instance-3");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group3");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.younho");
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        // container
        ContainerProperties containerProps = new ContainerProperties(this.mySubject);
        containerProps.setMessageListener(this);
        this.container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);

        // KafkaTemplate
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // start
        this.container.start();
    }

    @Override
    public void onMessage(ConsumerRecord<String, KafkaMsg> data) {
        byte[] correlationId = data.value().getKafkaCorrelationId();
        String replyTopic = data.value().getKafkaReplyTopic();
        String replyPartition = data.value().getKafkaReplyPartition();

        if (correlationId == null) {
            logger.info("[RCVD] message={}", data.value());
            return;
        }

        logger.info("[REPLY RCVD] message={}", data.value());

        if (correlationId != null && replyTopic != null) {
            // 응답 메시지 생성
            KafkaMsg message = new KafkaMsg();
            message.setStringField("REPLY_MESSAGE");
            message.setKafkaCorrelationId(correlationId);
            message.setKafkaReplyTopic(replyTopic);
            message.setKafkaReplyPartition(replyPartition);

            ProducerRecord<String, KafkaMsg> record = new ProducerRecord<>(replyTopic, message);

            // 응답 전송
            kafkaTemplate.send(record);
            logger.info("[REPLY SEND] message={}}", record);
        } else {
            // 오류 처리: 필요한 헤더가 없음
            logger.error("No Headers found");
        }
    }
}
