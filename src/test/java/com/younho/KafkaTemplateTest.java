package com.younho;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(topics = {"my-subject", "dest-subject"})
public class KafkaTemplateTest {
    @Autowired
    EmbeddedKafkaBroker broker;

    BlockingQueue<ConsumerRecord<String, KafkaMsg>> records;

    KafkaConfig kafkaConfig;
    KafkaWrapper kafkaWrapper;

    @BeforeEach
    public void setup() {
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
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, KafkaMsg>) record -> {
            System.out.println(record);
            records.add(record);
        });
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMsgSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaMsgDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        Map<String, Object> replyConsumerProps = new HashMap<>();
        replyConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        replyConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        replyConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        replyConsumerProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        replyConsumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaMsgDeserializer.class);
        replyConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group" + "-reply");
        replyConsumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        replyConsumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        kafkaConfig = new KafkaConfig();
        kafkaConfig.setMySubject("my-subject");
        kafkaConfig.setDestSubject("dest-subject");
        kafkaConfig.setProducerProps(producerProps);
        kafkaConfig.setConsumerProps(consumerProps);
        kafkaConfig.setReplyConsumerProps(replyConsumerProps);

        kafkaWrapper = kafkaConfig.createInstance();
        kafkaWrapper.init();
    }

    @Test
    @DirtiesContext
    public void testSendMessage() throws InterruptedException {
        KafkaMsg kafkaMsg = new KafkaMsg();
        kafkaMsg.update("TEST_KEY", "TEST_VALUE");

        kafkaWrapper.send(kafkaMsg);

        ConsumerRecord<String, KafkaMsg> received = records.poll(10, TimeUnit.SECONDS);

        assertNotNull(received, "Record not received");
        assertEquals("TEST_VALUE", received.value().get("TEST_KEY"));
    }

    @Test
    @DirtiesContext
    public void testSendMessage_withHeaders() throws InterruptedException {
        KafkaMsg kafkaMsg = new KafkaMsg();
        kafkaMsg.update("TEST_KEY", "TEST_VALUE");
        kafkaMsg.setCorrelationId("TEST_CORRELATION_ID".getBytes());
        kafkaMsg.setReplyTopic("TEST_REPLY_TOPIC");
        kafkaMsg.setReplyPartition(1);

        kafkaWrapper.send(kafkaMsg);

        ConsumerRecord<String, KafkaMsg> received = records.poll(10, TimeUnit.SECONDS);

        assertNotNull(received, "Record not received");
        assertEquals("TEST_VALUE", received.value().get("TEST_KEY"));
        assertArrayEquals("TEST_CORRELATION_ID".getBytes(), received.value().getCorrelationId());
        assertEquals("TEST_REPLY_TOPIC", received.value().getReplyTopic());
        assertEquals(1, received.value().getReplyPartition());
    }

}