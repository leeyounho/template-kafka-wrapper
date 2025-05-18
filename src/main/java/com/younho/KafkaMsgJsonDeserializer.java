package com.younho;

import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.nio.charset.StandardCharsets;

public class KafkaMsgJsonDeserializer extends JsonDeserializer<KafkaMsg> {

    public KafkaMsgJsonDeserializer() {
        super(KafkaMsg.class); // KafkaMsg 타입 지정
    }

    @Override
    public KafkaMsg deserialize(String topic, Headers headers, byte[] data) {
        KafkaMsg msg = super.deserialize(topic, headers, data); // 기본 Jackson 역직렬화

        if (msg != null && headers != null) {
            if (headers.lastHeader(KafkaHeaders.REPLY_TOPIC) != null) {
                msg.setKafkaReplyTopic(new String(headers.lastHeader(KafkaHeaders.REPLY_TOPIC).value()));
            }
            if (headers.lastHeader(KafkaHeaders.CORRELATION_ID) != null) {
                msg.setKafkaCorrelationId(headers.lastHeader(KafkaHeaders.CORRELATION_ID).value());
            }
            if (headers.lastHeader(KafkaHeaders.REPLY_PARTITION) != null) {
                msg.setKafkaReplyPartition(new String(headers.lastHeader(KafkaHeaders.REPLY_PARTITION).value()));
            }
        }

        return msg;
    }
}
