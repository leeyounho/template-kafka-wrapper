package com.younho.util;

import com.younho.kafka.KafkaMsg;
import com.younho.kafka.KafkaMsgDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestConsumerUtil {

    public static class TestConsumerSetup {
        private final KafkaMessageListenerContainer<String, KafkaMsg> container;
        private final BlockingQueue<ConsumerRecord<String, KafkaMsg>> records;

        public TestConsumerSetup(KafkaMessageListenerContainer<String, KafkaMsg> container, BlockingQueue<ConsumerRecord<String, KafkaMsg>> records) {
            this.container = container;
            this.records = records;
        }

        public KafkaMessageListenerContainer<String, KafkaMsg> getContainer() {
            return container;
        }

        public BlockingQueue<ConsumerRecord<String, KafkaMsg>> getRecords() {
            return records;
        }
    }

    public static TestConsumerSetup setupTestConsumer(
            EmbeddedKafkaBroker broker, String topic, String groupIdSuffix) {

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testConsumerGroup-" + groupIdSuffix, "false", broker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaMsgDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        DefaultKafkaConsumerFactory<String, KafkaMsg> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(topic);
        KafkaMessageListenerContainer<String, KafkaMsg> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        BlockingQueue<ConsumerRecord<String, KafkaMsg>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, KafkaMsg>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());

        return new TestConsumerSetup(container, records);
    }
}