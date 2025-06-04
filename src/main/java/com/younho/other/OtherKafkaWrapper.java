package com.younho.other;

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
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getConsumerProps());

        ContainerProperties containerProps = new ContainerProperties(kafkaConfig.getMySubject());
        containerProps.setMessageListener(this);
        this.container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        this.replyConsumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfig.getReplyConsumerProps());

        ContainerProperties replyContainerProps = new ContainerProperties(kafkaConfig.getMySubject());
        KafkaMessageListenerContainer<String, OtherKafkaMsg> replyContainer = new KafkaMessageListenerContainer<>(replyConsumerFactory, replyContainerProps);

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
        // 1. Listener Containers 중지
        if (this.replyingKafkaTemplate != null && this.replyingKafkaTemplate.isRunning()) {
            this.replyingKafkaTemplate.stop(); // 내부 Reply Listener Container 중지
        }
        if (this.container != null && this.container.isRunning()) {
            this.container.stop();
        }

        // 2. ProducerFactory 해제 (kafkaTemplate과 replyingKafkaTemplate이 공유)
        // DefaultKafkaProducerFactory의 destroy()는 여러 번 호출해도 안전합니다 (내부적으로 'closed' 플래그 체크).
        if (this.producerFactory instanceof DefaultKafkaProducerFactory) {
            ((DefaultKafkaProducerFactory<?, ?>) this.producerFactory).destroy();
        }

        // DefaultKafkaConsumerFactory는 DisposableBean을 구현하지 않으므로,
        // 컨테이너 중지로 인해 내부 컨슈머들이 닫히면서 관련 리소스가 정리됩니다.
        // 따라서 consumerFactory 및 replyConsumerFactory에 대한 별도 destroy 호출은 필요하지 않습니다.
    }

    @Override
    public void onMessage(ConsumerRecord<String, OtherKafkaMsg> data) {
        if (data.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null) return; // reply 메시지 처리는 replyContainer 에서 처리하기 위해 무시
        OtherKafkaMsg message = data.value();
        logger.info("[onMessage] topic={}, message={} correlationId={}", data.topic(), message, data.headers().lastHeader(KafkaHeaders.CORRELATION_ID));
    }

    public void sendAsync(OtherKafkaMsg message) {
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

    public void send(OtherKafkaMsg message) {
        try {
            kafkaTemplate.send(kafkaConfig.getDestSubject(), message).get(kafkaConfig.getTimeout(), TimeUnit.SECONDS);
            logger.info("[send] topic={} message={}", kafkaConfig.getDestSubject(), message);
        } catch (Exception e) {
            logger.error("[send] send failed", e);
        }
    }

    public OtherKafkaMsg sendRequest(OtherKafkaMsg message) {
        OtherKafkaMsg replyMessage = null;
        try {
            ProducerRecord<String, OtherKafkaMsg> record = new ProducerRecord<>(kafkaConfig.getDestSubject(), message);
            RequestReplyFuture<String, OtherKafkaMsg, OtherKafkaMsg> reply = replyingKafkaTemplate.sendAndReceive(record, Duration.ofSeconds(60));
            SendResult<String, OtherKafkaMsg> sendResult = reply.getSendFuture().get();
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
