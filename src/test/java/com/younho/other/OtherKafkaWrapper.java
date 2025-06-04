package com.younho.other;

import com.younho.kafka.KafkaConfig;
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

public class OtherKafkaWrapper implements MessageListener<String, OtherKafkaMsg> {
    private static final Logger logger = LoggerFactory.getLogger(OtherKafkaWrapper.class);

    private OtherKafkaConfig kafkaConfig;

    private ProducerFactory<String, OtherKafkaMsg> producerFactory;
    private ConsumerFactory<String, OtherKafkaMsg> consumerFactory;
    private ConsumerFactory<String, OtherKafkaMsg> replyConsumerFactory;
    private KafkaMessageListenerContainer<String, OtherKafkaMsg> container;
    private KafkaTemplate<String, OtherKafkaMsg> kafkaTemplate;
    private ReplyingKafkaTemplate<String, OtherKafkaMsg, OtherKafkaMsg> replyingKafkaTemplate;

    private MicrometerProducerListener producerMeterRegistry;
    private MicrometerConsumerListener consumerMeterRegistry;

    public OtherKafkaWrapper(OtherKafkaConfig kafkaConfig) {
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
        KafkaMessageListenerContainer<String, OtherKafkaMsg> replyContainer = new KafkaMessageListenerContainer<>(replyConsumerFactory, replyContainerProps);
        this.replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        this.replyingKafkaTemplate.setSharedReplyTopic(true);
        this.replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(kafkaConfig.getSendSyncTimeout()));

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
    public void onMessage(ConsumerRecord<String, OtherKafkaMsg> data) {
        if (data.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null) return; // reply 메시지 처리는 replyContainer 에서 처리하기 위해 무시
        OtherKafkaMsg message = data.value();
        logger.info("[onMessage] topic={}, message={} correlationId={}", data.topic(), message, data.headers().lastHeader(KafkaHeaders.CORRELATION_ID));
    }

    public void send(OtherKafkaMsg message) {
        try {
            kafkaTemplate.send(kafkaConfig.getDestSubject(), message).get(kafkaConfig.getSendSyncTimeout(), TimeUnit.SECONDS);
            logger.info("[send] topic={} message={}", kafkaConfig.getDestSubject(), message);
        } catch (Exception e) {
            logger.error("[send] send failed", e);
        }
    }

    public OtherKafkaMsg sendRequest(OtherKafkaMsg message) {
        OtherKafkaMsg replyMessage = null;
        try {
            ProducerRecord<String, OtherKafkaMsg> record = new ProducerRecord<>(kafkaConfig.getDestSubject(), message);
            RequestReplyFuture<String, OtherKafkaMsg, OtherKafkaMsg> reply = replyingKafkaTemplate.sendAndReceive(record);
            SendResult<String, OtherKafkaMsg> sendResult = reply.getSendFuture().get();
            logger.info("[sendRequest] correlationId={} topic={} message={}", sendResult.getProducerRecord().headers().lastHeader(KafkaHeaders.CORRELATION_ID).value(), kafkaConfig.getDestSubject(), message);
            replyMessage = reply.get(kafkaConfig.getSendSyncTimeout(), TimeUnit.SECONDS).value();
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
