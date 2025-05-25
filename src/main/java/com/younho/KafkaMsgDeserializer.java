package com.younho;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class KafkaMsgDeserializer extends JsonDeserializer<KafkaMsg> {

    public KafkaMsgDeserializer() {
        super(KafkaMsg.class);
    }

    @Override
    public KafkaMsg deserialize(String topic, Headers headers, byte[] data) {
        // KafkaMsg 전체(JSON)를 역직렬화
        return super.deserialize(topic, headers, data);
    }
}