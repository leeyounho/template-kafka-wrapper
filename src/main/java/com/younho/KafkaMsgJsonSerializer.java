package com.younho;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.charset.StandardCharsets;

public class KafkaMsgJsonSerializer extends JsonSerializer<KafkaMsg> {
    @Override
    public byte[] serialize(String topic, Headers headers, KafkaMsg data) {
        if (data != null) {
            if (data.getKafkaReplyTopic() != null) {
                headers.add(KafkaHeaders.REPLY_TOPIC, data.getKafkaReplyTopic().getBytes());
            }
            if (data.getKafkaCorrelationId() != null) {
                headers.add(KafkaHeaders.CORRELATION_ID, data.getKafkaCorrelationId());
            }
            if (data.getKafkaReplyPartition() != null) {
                headers.add(KafkaHeaders.REPLY_PARTITION, data.getKafkaReplyPartition().getBytes());
            }
        }

        return super.serialize(topic, headers, data);
    }
}
