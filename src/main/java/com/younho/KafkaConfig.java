package com.younho;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Scope("prototype")
public class KafkaConfig {
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

    public KafkaConfig() {
        // TODO bootstrapserver를 생성자에서 받으면 properties를 여기서 초기화해도 됨
    }

    public KafkaWrapper createInstance() {
        KafkaWrapper kafkaWrapper = new KafkaWrapper(this);

        if (kafkaProducerMeterRegistry != null) kafkaWrapper.setProducerMeterRegistry(new MicrometerProducerListener<>(kafkaProducerMeterRegistry));
        if (kafkaConsumerMeterRegistry != null) kafkaWrapper.setConsumerMeterRegistry(new MicrometerConsumerListener<>(kafkaConsumerMeterRegistry));

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
