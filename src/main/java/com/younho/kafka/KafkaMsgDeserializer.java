package com.younho.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class KafkaMsgDeserializer extends JsonDeserializer<KafkaMsg> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMsgDeserializer.class);

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

        if (logger.isDebugEnabled()) {
            logger.debug("Raw JSON data received for topic {}: {}", topic, new String(data));
        }

        KafkaMsg msg = super.deserialize(topic, kafkaHeaders, data);

        if (msg == null) {
            return null;
        }

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