package com.younho;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class KafkaWrapper<T> implements MessageListener<String, T> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaWrapper.class);

    private KafkaConfig kafkaConfig;

    private ProducerFactory<String, T> producerFactory;
    private ConsumerFactory<String, T> consumerFactory;
    private ConsumerFactory<String, T> replyConsumerFactory;
    private KafkaMessageListenerContainer<String, T> container;
    private KafkaTemplate<String, T> kafkaTemplate;
    private ReplyingKafkaTemplate<String, T, T> replyingKafkaTemplate;

    private MicrometerProducerListener producerMeterRegistry;
    private MicrometerConsumerListener consumerMeterRegistry;

    public KafkaWrapper(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void init() {
        this.producerFactory = new DefaultKafkaProducerFactory<>(kafkaConfig.getProducerProps());
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getConsumerProps());

        // NOTE debug 용
//        producerFactory.getConfigurationProperties().forEach((k, v) -> logger.info("ProducerConfig: {}={}", k, v));
//        consumerFactory.getConfigurationProperties().forEach((k, v) -> logger.info("ConsumerConfig: {}={}", k, v));

        ContainerProperties containerProps = new ContainerProperties(kafkaConfig.getMySubject());
        containerProps.setMessageListener(this);
        this.container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        this.replyConsumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getReplyConsumerProps());

        ContainerProperties replyContainerProps = new ContainerProperties(kafkaConfig.getMySubject());
        KafkaMessageListenerContainer<String, T> replyContainer = new KafkaMessageListenerContainer<>(replyConsumerFactory, replyContainerProps);

        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
        this.replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        replyingKafkaTemplate.setSharedReplyTopic(true); // NOTE topic 공유

        // register metric
        if (producerMeterRegistry != null) producerFactory.addListener(producerMeterRegistry);
        if (consumerMeterRegistry != null) consumerFactory.addListener(consumerMeterRegistry);

        // start
        this.container.start();
        this.replyingKafkaTemplate.start();
    }

    // TODO stop 으로 하는게 좋은지?
    public void destroy() {
        if (container != null) container.destroy();
        if (kafkaTemplate != null) kafkaTemplate.destroy();
        if (replyingKafkaTemplate != null) replyingKafkaTemplate.destroy();
    }

    @Override
    public void onMessage(ConsumerRecord<String, T> data) {
        if (data.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null) return; // reply 메시지 처리는 replyContainer 에서 처리하기 위해 무시
        T message = data.value();
        logger.info("[RCVD] topic={}, message={} correlationId={}", data.topic(), message, data.headers().lastHeader(KafkaHeaders.CORRELATION_ID));
    }

    public void sendKafkaAsynchronous(T message) {
        kafkaTemplate.send(kafkaConfig.getDestSubject(), message)
                .addCallback(
                        result -> logger.info("[SEND] topic={} message={}", kafkaConfig.getDestSubject(), message),
                        ex -> logger.error("[SEND] send failed", ex)
                );
    }

    public void sendKafka(T message) {
        try {
            SendResult<String, T> sendResult = kafkaTemplate.send(kafkaConfig.getDestSubject(), message).get(kafkaConfig.getTimeout(), TimeUnit.SECONDS);
            logger.info("[SEND] topic={} message={}", kafkaConfig.getDestSubject(), message);
        } catch (Exception e) {
            logger.error("[SEND] send failed", e);
        }
    }

    public T sendKafkaRequest(T message) {
        T replyMessage = null;
        try {
            ProducerRecord<String, T> record = new ProducerRecord<>(kafkaConfig.getDestSubject(), message);
            RequestReplyFuture<String, T, T> reply = replyingKafkaTemplate.sendAndReceive(record, Duration.ofSeconds(60));
            SendResult<String, T> sendResult = reply.getSendFuture().get();
            logger.info("[REQUEST] correlationId={} topic={} message={}", sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value(), kafkaConfig.getDestSubject(), message);
            replyMessage = reply.get(kafkaConfig.getTimeout(), TimeUnit.SECONDS).value();
            logger.info("[REQUEST] replyMessage={}", replyMessage);
        } catch (Exception e) {
            logger.error("[REQUEST] request failed", e);
        }
        return replyMessage;
    }

    public void setProducerMeterRegistry(MicrometerProducerListener producerMeterRegistry) {
        this.producerMeterRegistry = producerMeterRegistry;
    }

    public void setConsumerMeterRegistry(MicrometerConsumerListener consumerMeterRegistry) {
        this.consumerMeterRegistry = consumerMeterRegistry;
    }
}
