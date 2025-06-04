package com.younho;

import com.younho.kafka.KafkaConfig;
import com.younho.kafka.KafkaMsg;
import com.younho.kafka.KafkaMsgDeserializer;
import com.younho.kafka.KafkaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
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

    KafkaMessageListenerContainer<String, KafkaMsg> container;
    BlockingQueue<ConsumerRecord<String, KafkaMsg>> records;

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
        container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, KafkaMsg>) record -> {
            System.out.println(record);
            records.add(record);
        });
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());

        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setBootstrapServers(broker.getBrokersAsString());
        kafkaConfig.setConsumerGroupId("my-group");
        kafkaConfig.setReplyConsumerGroupId("my-group-reply");
        kafkaConfig.setMySubject("my-subject");
        kafkaConfig.setDestSubject("dest-subject");
        kafkaWrapper = kafkaConfig.createInstance();
        kafkaWrapper.init();
    }

    @AfterEach
    public void tearDown() {
        if (kafkaWrapper != null) {
            kafkaWrapper.destroy();
        }
        if (container != null && container.isRunning()) {
            container.stop();
        }
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