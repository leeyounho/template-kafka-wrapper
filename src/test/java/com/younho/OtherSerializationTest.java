package com.younho;

import com.younho.kafka.KafkaConfig;
import com.younho.kafka.KafkaMsg;
import com.younho.kafka.KafkaMsgDeserializer;
import com.younho.other.OtherKafkaConfig;
import com.younho.other.OtherKafkaMsg;
import com.younho.other.OtherKafkaWrapper;
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
@DirtiesContext
@EmbeddedKafka(topics = {"my-subject", "dest-subject"})
public class OtherSerializationTest {
    @Autowired
    EmbeddedKafkaBroker broker;

    KafkaMessageListenerContainer<String, KafkaMsg> container;
    BlockingQueue<ConsumerRecord<String, KafkaMsg>> records;

    OtherKafkaConfig kafkaConfig;
    OtherKafkaWrapper kafkaWrapper;

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

        OtherKafkaConfig kafkaConfig = new OtherKafkaConfig();
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
    public void testSendMessage() throws InterruptedException {
        OtherKafkaMsg kafkaMsg = new OtherKafkaMsg();
        kafkaMsg.update("TEST_BYTES_KEY", "TEST_BYTES_VALUE".getBytes());
        kafkaMsg.update("TEST_STRING_KEY", "TEST_STRING_VALUE");
        kafkaMsg.update("TEST_INTEGER_KEY", 1);
        kafkaMsg.update("TEST_INTEGER_LIKE_STRING_KEY", "2");

        kafkaWrapper.send(kafkaMsg);

        ConsumerRecord<String, KafkaMsg> received = records.poll(10, TimeUnit.SECONDS);

        assertNotNull(received, "Record not received");
        assertArrayEquals("TEST_BYTES_VALUE".getBytes(), (byte[]) received.value().get("TEST_BYTES_KEY"));
        assertEquals("TEST_STRING_VALUE", received.value().get("TEST_STRING_KEY"));
        assertEquals(1, received.value().get("TEST_INTEGER_KEY"));
        assertEquals("2", received.value().get("TEST_INTEGER_LIKE_STRING_KEY"));
    }
}