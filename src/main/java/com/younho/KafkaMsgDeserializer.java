package com.younho;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.errors.SerializationException;
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
    }

//    @Override
//    public KafkaMsg deserialize(String topic, Headers kafkaHeaders, byte[] data) {
//        KafkaMsg msg = super.deserialize(topic, kafkaHeaders, data);
//        if (msg == null) return null;
//
//        // Base64 String -> byte[] 변환
//        Map<String, Object> valueMap = msg.getValue();
//        if (valueMap != null) {
//            for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
//                if (entry.getValue() instanceof String) {
//                    String val = (String) entry.getValue();
//                    // Base64 String이면 decode
//                    try {
//                        byte[] decoded = Base64.getDecoder().decode(val);
//                        entry.setValue(decoded);
//                    } catch (IllegalArgumentException ignored) {
//                    }
//                }
//            }
//        }
//
//        // Header 변환
//        Map<String, byte[]> headerMap = new HashMap<>();
//        for (Header header : kafkaHeaders) {
//            headerMap.put(header.key(), header.value());
//        }
//        msg.setHeaders(headerMap);
//
//        return msg;
//    }

    @Override
    public KafkaMsg deserialize(String topic, Headers kafkaHeaders, byte[] data) {
        System.out.println("Raw JSON data received: " + new String(data, java.nio.charset.StandardCharsets.UTF_8));

        if (data == null || data.length == 0) {
            // 원본 코드에서 super.deserialize()가 null을 반환할 수 있었던 것처럼,
            // 데이터가 없으면 null 반환 (또는 예외 처리)
            return null;
        }

        KafkaMsg msg = new KafkaMsg();
        Map<String, Object> valueMap;

        try {
            // JsonDeserializer로부터 ObjectMapper를 가져와 byte[] 데이터를 Map<String, Object>으로 변환
            valueMap = this.objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            // 역직렬화 실패 시 SerializationException 발생 또는 null 반환
            // 여기서는 예외를 던져 ErrorHandlingDeserializer가 처리하도록 유도할 수 있습니다.
            throw new SerializationException("Error deserializing JSON to Map<String, Object>", e);
        }

        // valueMap이 null인 경우는 getObjectMapper().readValue()에서 예외가 발생하므로 보통 도달하지 않음.
        if (valueMap == null) {
            return null; // 방어적 코딩
        }

        msg.setValue(valueMap);

        // 기존 Base64 문자열 -> byte[] 변환 로직 (valueMap에 대해 수행)
        // msg.getValue()는 방금 설정한 valueMap을 반환합니다.
        Map<String, Object> mapToProcess = msg.getValue();
        if (mapToProcess != null) {
            for (Map.Entry<String, Object> entry : mapToProcess.entrySet()) {
                if (entry.getValue() instanceof String) {
                    String val = (String) entry.getValue();
                    try {
                        byte[] decoded = Base64.getDecoder().decode(val);
                        entry.setValue(decoded); // 맵의 값을 직접 수정
                    } catch (IllegalArgumentException ignored) {
                        // Base64 문자열이 아니면 원래 문자열 값을 유지
                    }
                }
            }
        }

        // 기존 Header 변환 로직
        Map<String, byte[]> headerMap = new HashMap<>();
        if (kafkaHeaders != null) {
            for (Header header : kafkaHeaders) {
                headerMap.put(header.key(), header.value());
            }
        }
        msg.setHeaders(headerMap);

        return msg;
    }
}