package com.younho;

import com.younho.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(topics = {"my-subject", "dest-subject"})
public class ReplyingKafkaTemplateTest {
    @Autowired
    EmbeddedKafkaBroker broker;

    KafkaWrapper kafkaWrapper;

    @BeforeEach
    public void setup() {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setBootstrapServers(broker.getBrokersAsString());
        kafkaConfig.setConsumerGroupId("my-group");
        kafkaConfig.setReplyConsumerGroupId("my-group-reply");
        kafkaConfig.setMySubject("my-subject");
        kafkaConfig.setDestSubject("dest-subject");
        kafkaWrapper = kafkaConfig.createInstance();
        kafkaWrapper.init();
    }

    @Test
    public void testRequestMessage() {
        // test consumer
        Map<String, Object> testConsumerProps = KafkaTestUtils.consumerProps("testT", "false", broker);
        testConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        testConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        testConsumerProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        testConsumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaMsgDeserializer.class);
        testConsumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        testConsumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        DefaultKafkaConsumerFactory<String, KafkaMsg> cf = new DefaultKafkaConsumerFactory<>(testConsumerProps);
        ContainerProperties containerProperties = new ContainerProperties("dest-subject");
        KafkaMessageListenerContainer<String, KafkaMsg> container = new KafkaMessageListenerContainer<>(cf, containerProperties);

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMsgSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        ProducerFactory<String, KafkaMsg> pf = new DefaultKafkaProducerFactory<String, KafkaMsg>(producerProps);
        KafkaTemplate<String, KafkaMsg> kafkaTemplate = new KafkaTemplate<>(pf);

        container.setupMessageListener((MessageListener<String, KafkaMsg>) record -> {
            System.out.println(record);

            byte[] correlationId = record.value().getCorrelationId();
            KafkaMsg kafkaMsg = new KafkaMsg();
            kafkaMsg.setCorrelationId(correlationId);
            kafkaMsg.update("PONG_KEY", "PONG_VALUE");

            kafkaTemplate.send("my-subject", kafkaMsg);
        });
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());

        KafkaMsg kafkaMsg = new KafkaMsg();
        kafkaMsg.update("PING_KEY", "PING_VALUE");

        KafkaMsg received = kafkaWrapper.sendRequest(kafkaMsg);

        assertNotNull(received, "Record not received");
        assertNotNull(received.getCorrelationId(), "CorrelationId not null");
        assertEquals("PONG_VALUE", received.get("PONG_KEY"));
    }
}