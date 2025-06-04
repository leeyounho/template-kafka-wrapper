package com.younho.other;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class OtherKafkaMsgSerializer extends JsonSerializer<OtherKafkaMsg> {
    @Override
    public byte[] serialize(String topic, Headers kafkaHeaders, OtherKafkaMsg data) {
//        if (data != null && data.getHeaders() != null) {
//            // KafkaMsg의 headers를 Kafka native header로 복사
//            for (Map.Entry<String, byte[]> entry : data.getHeaders().entrySet()) {
//                if (entry.getValue() != null) {
//                    kafkaHeaders.remove(entry.getKey()); // 중복 방지 (이미 있으면 삭제)
//                    kafkaHeaders.add(new RecordHeader(entry.getKey(), entry.getValue()));
//                }
//            }
//        }
        // value(Map<String, Object> 등)를 직렬화
        return super.serialize(topic, kafkaHeaders, data);
    }
}