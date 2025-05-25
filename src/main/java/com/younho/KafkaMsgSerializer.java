package com.younho;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class KafkaMsgSerializer extends JsonSerializer<KafkaMsg> {

    @Override
    public byte[] serialize(String topic, Headers headers, KafkaMsg data) {
        // Kafka native header는 그대로 두고, KafkaMsg 내의 value(Map)와 key를 JSON으로 직렬화
        return super.serialize(topic, headers, data);
    }
}