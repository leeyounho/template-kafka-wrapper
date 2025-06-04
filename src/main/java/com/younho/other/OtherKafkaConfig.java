package com.younho.other;

import com.younho.kafka.KafkaMsgDeserializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Scope("prototype")
public class OtherKafkaConfig {
    private String bootstrapServers;
    private String mySubject;
    private String destSubject;
    private long timeout = 60L;

    private Map<String, Object> producerProps = new HashMap<>();
    private Map<String, Object> consumerProps = new HashMap<>();
    private Map<String, Object> replyConsumerProps = new HashMap<>();

    @Autowired(required = false)
    private MeterRegistry kafkaProducerMeterRegistry;

    @Autowired(required = false)
    private MeterRegistry kafkaConsumerMeterRegistry;

    public OtherKafkaConfig() {
        // 공통 프로듀서 속성 기본값 설정
        initializeDefaultProducerProps();

        // 공통 컨슈머 속성 기본값 설정
        initializeDefaultConsumerProps(consumerProps);
        initializeDefaultConsumerProps(replyConsumerProps);
    }

    private void initializeDefaultProducerProps() {
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OtherKafkaMsgSerializer.class); // 사용자 정의 직렬화 클래스
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // TODO
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    }

    private void initializeDefaultConsumerProps(Map<String, Object> props) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaMsgDeserializer.class); // 사용자 정의 역직렬화 클래스
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.replyConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public void setConsumerGroupId(String groupId) {
        this.consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public void setReplyConsumerGroupId(String groupId) {
        this.replyConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public OtherKafkaWrapper createInstance() {
        if (bootstrapServers == null || mySubject == null || destSubject == null || !consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG) || !replyConsumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalStateException("KafkaConfig is not fully initialized. Bootstrap servers, subjects, and group IDs must be set.");
        }

        OtherKafkaWrapper kafkaWrapper = new OtherKafkaWrapper(this);
        if (kafkaProducerMeterRegistry != null) {
            kafkaWrapper.setProducerMeterRegistry(new MicrometerProducerListener<>(kafkaProducerMeterRegistry));
        }
        if (kafkaConsumerMeterRegistry != null) {
            kafkaWrapper.setConsumerMeterRegistry(new MicrometerConsumerListener<>(kafkaConsumerMeterRegistry));
        }
        return kafkaWrapper;
    }

    public Map<String, Object> getConsumerProps() {
        return consumerProps;
    }

    public void setConsumerProps(Map<String, Object> consumerProps) {
        this.consumerProps = consumerProps;
    }

    public String getDestSubject() {
        return destSubject;
    }

    public void setDestSubject(String destSubject) {
        this.destSubject = destSubject;
    }

    public String getMySubject() {
        return mySubject;
    }

    public void setMySubject(String mySubject) {
        this.mySubject = mySubject;
    }

    public Map<String, Object> getProducerProps() {
        return producerProps;
    }

    public void setProducerProps(Map<String, Object> producerProps) {
        this.producerProps = producerProps;
    }

    public Map<String, Object> getReplyConsumerProps() {
        return replyConsumerProps;
    }

    public void setReplyConsumerProps(Map<String, Object> replyConsumerProps) {
        this.replyConsumerProps = replyConsumerProps;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
