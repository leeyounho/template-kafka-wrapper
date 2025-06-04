package com.younho.util;

import com.younho.kafka.KafkaConfig;
import com.younho.kafka.KafkaWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka
public abstract class AbstractKafkaTest {

    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    protected KafkaWrapper kafkaWrapper;
    protected KafkaConfig kafkaConfig;

    // 테스트에 사용될 공통 토픽 등을 상수로 정의
    protected static final String MY_SUBJECT = "my-subject";
    protected static final String DEST_SUBJECT = "dest-subject";
    protected static final String GROUP_ID = "test-group";
    protected static final String REPLY_GROUP_ID = "test-group-reply";


    @BeforeEach
    public void setUpKafkaWrapper() {
        kafkaConfig = new KafkaConfig();
        kafkaConfig.setBootstrapServers(embeddedKafkaBroker.getBrokersAsString());
        kafkaConfig.setConsumerGroupId(GROUP_ID);
        kafkaConfig.setReplyConsumerGroupId(REPLY_GROUP_ID); // 필요에 따라 설정
        kafkaConfig.setMySubject(MY_SUBJECT);         // KafkaWrapper가 수신할 토픽
        kafkaConfig.setDestSubject(DEST_SUBJECT);       // KafkaWrapper가 발신할 토픽
        // 필요시 MetricsConfig 등을 ApplicationContext에서 가져와 주입할 수 있음

        kafkaWrapper = kafkaConfig.createInstance();
        // MeterRegistry 주입 (실제 코드에서는 Spring Context에서 Bean을 가져오거나 Mock을 사용)
        // kafkaWrapper.setProducerMeterRegistry(new MicrometerProducerListener<>(new SimpleMeterRegistry()));
        // kafkaWrapper.setConsumerMeterRegistry(new MicrometerConsumerListener<>(new SimpleMeterRegistry()));
        kafkaWrapper.init();
    }

    @AfterEach
    public void tearDownKafkaWrapper() {
        if (kafkaWrapper != null) {
            kafkaWrapper.destroy();
        }
    }
}
