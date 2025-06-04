package com.younho.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KafkaMsg {
    private Map<String, Object> value = new HashMap<>();
    private Map<String, byte[]> headers = new HashMap<>();

    public KafkaMsg() {
    }

    @JsonCreator
    public KafkaMsg(Map<String, Object> value) {
        // 'headers'는 필드 선언 시 이미 초기화되었습니다.
        if (value != null) {
            this.value = new HashMap<>(value); // 외부 변경으로부터 안전하도록 방어적 복사
        } else {
            this.value = new HashMap<>(); // null 입력 시 빈 맵으로 초기화
        }
    }

    public Object get(String key) {
        return this.value.get(key);
    }

    public byte[] getCorrelationId() {
        return headers.get(KafkaHeaders.CORRELATION_ID);
    }

    public Map<String, byte[]> getHeaders() {
        return new HashMap<>(headers); // 방어적 복사 (getter)
    }

    @JsonIgnore
    public void setHeaders(Map<String, byte[]> headers) {
        if (headers != null) {
            this.headers = new HashMap<>(headers); // 방어적 복사 (setter)
        } else {
            this.headers = new HashMap<>();
        }
    }

    public Integer getReplyPartition() {
        byte[] val = headers.get(KafkaHeaders.REPLY_PARTITION);
        if (val == null || val.length != 4) return null;
        return ByteBuffer.wrap(val).getInt();
    }

    @JsonIgnore
    public void setReplyPartition(Integer partition) {
        if (partition != null) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(partition);
            headers.put(KafkaHeaders.REPLY_PARTITION, buffer.array());
        } else {
            headers.remove(KafkaHeaders.REPLY_PARTITION);
        }
    }

    public String getReplyTopic() {
        byte[] val = headers.get(KafkaHeaders.REPLY_TOPIC);
        return val == null ? null : new String(val, StandardCharsets.UTF_8);
    }

    @JsonIgnore
    public void setReplyTopic(String replyTopic) {
        if (replyTopic != null) {
            headers.put(KafkaHeaders.REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        } else {
            headers.remove(KafkaHeaders.REPLY_TOPIC);
        }
    }

    @JsonValue
    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        if (value != null) {
            this.value = new HashMap<>(value); // 방어적 복사
        } else {
            this.value = new HashMap<>();
        }
    }

    @JsonIgnore
    public void setCorrelationId(byte[] correlationId) {
        if (correlationId != null) {
            headers.put(KafkaHeaders.CORRELATION_ID, correlationId);
        } else {
            headers.remove(KafkaHeaders.CORRELATION_ID);
        }
    }

    public void update(String key, Object value) {
        this.value.put(key, value);
    }

    @Override
    public String toString() {
        // @JsonValue로 인해 로깅 시 value 맵이 주로 표현되므로,
        // 여기서는 headers 정보도 간략히 포함하거나 필요에 따라 커스터마이징
        return "KafkaMsg{" +
                "value=" + value + // Jackson이 이 부분을 직렬화할 것임
                ", headers_present=" + !headers.isEmpty() +
                (getCorrelationId() != null ? ", correlationId_present=true" : "") +
                '}';
    }
}