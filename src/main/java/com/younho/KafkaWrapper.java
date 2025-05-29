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

public class KafkaWrapper implements MessageListener<String, KafkaMsg> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaWrapper.class);

    private KafkaConfig kafkaConfig;

    private ProducerFactory<String, KafkaMsg> producerFactory;
    private ConsumerFactory<String, KafkaMsg> consumerFactory;
    private ConsumerFactory<String, KafkaMsg> replyConsumerFactory;
    private KafkaMessageListenerContainer<String, KafkaMsg> container;
    private KafkaTemplate<String, KafkaMsg> kafkaTemplate;
    private ReplyingKafkaTemplate<String, KafkaMsg, KafkaMsg> replyingKafkaTemplate;

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
        KafkaMessageListenerContainer<String, KafkaMsg> replyContainer = new KafkaMessageListenerContainer<>(replyConsumerFactory, replyContainerProps);

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

    public void destroy() {
        ((DefaultKafkaProducerFactory<?, ?>) kafkaTemplate.getProducerFactory()).destroy();
        ((DefaultKafkaProducerFactory<?, ?>) replyingKafkaTemplate.getProducerFactory()).destroy();
        container.stop();
    }

    @Override
    public void onMessage(ConsumerRecord<String, KafkaMsg> data) {
        if (data.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null) return; // reply 메시지 처리는 replyContainer 에서 처리하기 위해 무시
        KafkaMsg message = data.value();
        logger.info("[onMessage] topic={}, message={} correlationId={}", data.topic(), message, data.headers().lastHeader(KafkaHeaders.CORRELATION_ID));
    }

    public void sendAsync(KafkaMsg message) {
        try {
            kafkaTemplate.send(kafkaConfig.getDestSubject(), message)
                    .addCallback(
                            result -> logger.info("[send] topic={} message={}", kafkaConfig.getDestSubject(), message),
                            ex -> logger.error("[send] send async failed (callback)", ex)
                    );
        } catch (Exception e) {
            logger.error("[send] send async failed", e);
        }
    }

    public void send(KafkaMsg message) {
        try {
            kafkaTemplate.send(kafkaConfig.getDestSubject(), message).get(kafkaConfig.getTimeout(), TimeUnit.SECONDS);
            logger.info("[send] topic={} message={}", kafkaConfig.getDestSubject(), message);
        } catch (Exception e) {
            logger.error("[send] send failed", e);
        }
    }

    public KafkaMsg sendRequest(KafkaMsg message) {
        KafkaMsg replyMessage = null;
        try {
            ProducerRecord<String, KafkaMsg> record = new ProducerRecord<>(kafkaConfig.getDestSubject(), message);
            RequestReplyFuture<String, KafkaMsg, KafkaMsg> reply = replyingKafkaTemplate.sendAndReceive(record, Duration.ofSeconds(60));
            SendResult<String, KafkaMsg> sendResult = reply.getSendFuture().get();
            logger.info("[sendRequest] correlationId={} topic={} message={}", sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value(), kafkaConfig.getDestSubject(), message);
            replyMessage = reply.get(kafkaConfig.getTimeout(), TimeUnit.SECONDS).value();
            logger.info("[sendRequest] replyMessage={}", replyMessage);
        } catch (Exception e) {
            logger.error("[sendRequest] failed", e);
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
