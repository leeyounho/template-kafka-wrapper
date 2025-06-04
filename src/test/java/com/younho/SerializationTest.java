package com.younho;

import com.younho.kafka.KafkaMsg;
import com.younho.util.AbstractKafkaTest;
import com.younho.util.TestConsumerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class SerializationTest extends AbstractKafkaTest {
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
    public void testSendMessage() throws InterruptedException {
        KafkaMsg kafkaMsg = new KafkaMsg();
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