package com.younho.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class KafkaMsgSerializer extends JsonSerializer<KafkaMsg> {
    public KafkaMsgSerializer() {
        super(); // 부모 JsonSerializer의 기본 생성자 호출
        // Spring Kafka의 JsonSerializer가 타입 정보를 자동으로 추가하는 기능을 비활성화합니다.
        // 이렇게 하면 Jackson의 @JsonValue 동작에 더 집중할 수 있습니다.
        this.setAddTypeInfo(false);
    }


    @Override
    public byte[] serialize(String topic, Headers kafkaHeaders, KafkaMsg data) {
        if (data == null) {
            return super.serialize(topic, kafkaHeaders, null);
        }

        if (data != null && data.getHeaders() != null) {
            // KafkaMsg의 headers를 Kafka native header로 복사
            for (Map.Entry<String, byte[]> entry : data.getHeaders().entrySet()) {
                if (entry.getValue() != null) {
                    kafkaHeaders.remove(entry.getKey()); // 중복 방지 (이미 있으면 삭제)
                    kafkaHeaders.add(new RecordHeader(entry.getKey(), entry.getValue()));
                }
            }
        }
        // value(Map<String, Object> 등)를 직렬화
        return super.serialize(topic, kafkaHeaders, data);
    }
}