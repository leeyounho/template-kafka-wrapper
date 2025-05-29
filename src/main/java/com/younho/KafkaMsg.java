package com.younho;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

    public Map<String, byte[]> getHeaders() {
        return headers;
    }

    @JsonIgnore
    public void setHeaders(Map<String, byte[]> headers) {
        this.headers = headers;
    }

    public byte[] getCorrelationId() {
        return headers.get(KafkaHeaders.CORRELATION_ID);
    }

    @JsonIgnore
    public byte[] setCorrelationId(byte[] correlationId) {
        return headers.put(KafkaHeaders.CORRELATION_ID, correlationId);
    }

    // replyTopic도 보통 String (topic명)으로 사용
    public String getReplyTopic() {
        byte[] val = headers.get(KafkaHeaders.REPLY_TOPIC);
        return val == null ? null : new String(val, StandardCharsets.UTF_8);
    }

    @JsonIgnore
    public void setReplyTopic(String replyTopic) {
        headers.put(KafkaHeaders.REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
    }

    public Integer getReplyPartition() {
        byte[] val = headers.get(KafkaHeaders.REPLY_PARTITION);
        if (val == null || val.length != 4) return null;
        return ByteBuffer.wrap(val).getInt();
    }

    @JsonIgnore
    public void setReplyPartition(Integer partition) {
        if (partition == null) {
            headers.remove(KafkaHeaders.REPLY_PARTITION);
            return;
        }
        // int → 4-byte big-endian 변환
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(partition);
        headers.put(KafkaHeaders.REPLY_PARTITION, buffer.array());
    }
}