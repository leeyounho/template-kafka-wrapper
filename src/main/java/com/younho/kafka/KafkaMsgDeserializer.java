package com.younho.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class KafkaMsgDeserializer extends JsonDeserializer<KafkaMsg> {
    public KafkaMsgDeserializer() {
        super(KafkaMsg.class);
        this.setUseTypeHeaders(false);
        this.addTrustedPackages("*");
    }

    @Override
    public KafkaMsg deserialize(String topic, Headers kafkaHeaders, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        System.out.println("Raw JSON data received for topic " + topic + " " + new String(data));

        // 단계 1: JSON 페이로드를 KafkaMsg 객체로 역직렬화합니다.
        // super.deserialize()는 KafkaMsg.class의 @JsonCreator(Map<String, Object> value) 생성자를 호출하여,
        // JSON 데이터를 Map<String, Object>으로 변환 후 KafkaMsg 객체의 value 필드에 채워줍니다.
        // 이 과정 자체가 "스키마 프리"하게 JSON 객체를 Map으로 받는 것을 의미합니다.
        KafkaMsg msg = super.deserialize(topic, kafkaHeaders, data);

        if (msg == null) {
            // super.deserialize()가 null을 반환하는 경우 (예: JSON 파싱 실패 및 에러 핸들러가 null 반환)
            return null;
        }

        // 단계 2: value 맵의 값들에 대해 Base64 디코딩을 수행합니다.
        // msg.getValue()는 @JsonCreator를 통해 이미 채워진 value 맵을 반환합니다.
        Map<String, Object> valueMap = msg.getValue();
        if (valueMap != null) {
            for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                if (entry.getValue() instanceof String) {
                    String stringValue = (String) entry.getValue();
                    try {
                        byte[] decodedBytes = Base64.getDecoder().decode(stringValue);
                        entry.setValue(decodedBytes); // 맵의 값을 직접 수정
                    } catch (IllegalArgumentException e) {
                        // Base64 문자열이 아니면 원래 문자열 값을 유지 (예외 무시)
                    }
                }
            }
        }


        // 단계 3: Kafka 네이티브 헤더를 KafkaMsg 객체의 headers 필드에 설정합니다.
        Map<String, byte[]> nativeHeadersMap = new HashMap<>();
        if (kafkaHeaders != null) { // kafkaHeaders가 null일 수 있으므로 체크
            for (Header nativeHeader : kafkaHeaders) {
                nativeHeadersMap.put(nativeHeader.key(), nativeHeader.value());
            }
        }
        msg.setHeaders(nativeHeadersMap); // KafkaMsg.setHeaders는 방어적 복사를 수행해야 함

        return msg;
    }
}