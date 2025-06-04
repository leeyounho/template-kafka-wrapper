package com.younho.kafka;

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
        if (producerMeterRegistry != null) {
            producerFactory.addListener(producerMeterRegistry);
        }
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);


        this.consumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getConsumerProps());
        if (consumerMeterRegistry != null) {
            consumerFactory.addListener(consumerMeterRegistry);
        }
        ContainerProperties containerProps = new ContainerProperties(kafkaConfig.getMySubject());
        containerProps.setMessageListener(this);
        this.container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);

        this.replyConsumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getReplyConsumerProps());
        if (consumerMeterRegistry != null) { // 응답 컨슈머에도 동일 리스너 적용
            replyConsumerFactory.addListener(consumerMeterRegistry);
        }
        ContainerProperties replyContainerProps = new ContainerProperties(kafkaConfig.getMySubject());
        KafkaMessageListenerContainer<String, KafkaMsg> replyContainer = new KafkaMessageListenerContainer<>(replyConsumerFactory, replyContainerProps);
        this.replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        this.replyingKafkaTemplate.setSharedReplyTopic(true); // subject 공유
        this.replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(kafkaConfig.getTimeout())); // 기본 응답 타임아웃 설정

        // start
        this.container.start();
        this.replyingKafkaTemplate.start();
    }

    public void destroy() {
        if (this.replyingKafkaTemplate != null && this.replyingKafkaTemplate.isRunning()) {
            this.replyingKafkaTemplate.stop();
            logger.info("ReplyingKafkaTemplate stopped.");
        }
        if (this.container != null && this.container.isRunning()) {
            this.container.stop();
            logger.info("KafkaMessageListenerContainer stopped.");
        }
        if (this.producerFactory instanceof DefaultKafkaProducerFactory) {
            ((DefaultKafkaProducerFactory<?, ?>) this.producerFactory).destroy();
            logger.info("ProducerFactory destroyed.");
        }
    }

    @Override
    public void onMessage(ConsumerRecord<String, KafkaMsg> data) {
        if (data.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null) return; // reply 메시지 처리는 replyContainer 에서 처리하기 위해 무시
        KafkaMsg message = data.value();
        logger.info("[onMessage] topic={}, message={} correlationId={}", data.topic(), message, data.headers().lastHeader(KafkaHeaders.CORRELATION_ID));
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
