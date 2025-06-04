package com.younho;

import com.younho.kafka.KafkaMsg;
import com.younho.util.AbstractKafkaTest;
import com.younho.util.TestConsumerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaTemplateTest extends AbstractKafkaTest {
    KafkaMessageListenerContainer<String, KafkaMsg> container;
    BlockingQueue<ConsumerRecord<String, KafkaMsg>> records;

    @BeforeEach
    public void setupLocal() { // AbstractKafkaIntegrationTest의 setUpKafkaWrapper 이후 실행됨
        TestConsumerUtil.TestConsumerSetup testConsumerSetup = TestConsumerUtil.setupTestConsumer(embeddedKafkaBroker, DEST_SUBJECT, "templateTest");
        container = testConsumerSetup.getContainer();
        records = testConsumerSetup.getRecords();
    }

    @AfterEach
    public void tearDownLocal() {
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