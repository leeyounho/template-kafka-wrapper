package com.younho;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaMsgDeserializer extends JsonDeserializer<KafkaMsg> {

    public KafkaMsgDeserializer() {
        super(KafkaMsg.class);
    }

    @Override
    public KafkaMsg deserialize(String topic, Headers kafkaHeaders, byte[] data) {
        KafkaMsg msg = super.deserialize(topic, kafkaHeaders, data);
        if (msg == null) return null;

        Map<String, byte[]> headerMap = new HashMap<>();
        for (Header header : kafkaHeaders) {
            headerMap.put(header.key(), header.value());
        }
        msg.setHeaders(headerMap);

        return msg;
    }
}