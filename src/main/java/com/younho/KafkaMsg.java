package com.younho;

import java.util.Arrays;

public class KafkaMsg {
    private String kafkaReplyTopic;
    private byte[] kafkaCorrelationId;
    private String kafkaReplyPartition;

    String stringField;
    Object objectField;
    boolean booleanField;
    byte[] byteArrayField;

    public byte[] getByteArrayField() {
        return byteArrayField;
    }

    public void setByteArrayField(byte[] byteArrayField) {
        this.byteArrayField = byteArrayField;
    }

    public Object getObjectField() {
        return objectField;
    }

    public void setObjectField(Object objectField) {
        this.objectField = objectField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public boolean isBooleanField() {
        return booleanField;
    }

    public void setBooleanField(boolean booleanField) {
        this.booleanField = booleanField;
    }

    public String getKafkaReplyTopic() {
        return kafkaReplyTopic;
    }

    public void setKafkaReplyTopic(String kafkaReplyTopic) {
        this.kafkaReplyTopic = kafkaReplyTopic;
    }

    public byte[] getKafkaCorrelationId() {
        return kafkaCorrelationId;
    }

    public void setKafkaCorrelationId(byte[] kafkaCorrelationId) {
        this.kafkaCorrelationId = kafkaCorrelationId;
    }

    public String getKafkaReplyPartition() {
        return kafkaReplyPartition;
    }

    public void setKafkaReplyPartition(String kafkaReplyPartition) {
        this.kafkaReplyPartition = kafkaReplyPartition;
    }

    @Override
    public String toString() {
        return "KafkaMsg{" +
                "replyTopic='" + kafkaReplyTopic + '\'' +
                ", correlationId='" + kafkaCorrelationId + '\'' +
                ", replyPartition=" + kafkaReplyPartition +
                ", stringField='" + stringField + '\'' +
                ", objectField=" + objectField +
                ", booleanField=" + booleanField +
                ", byteArrayField=" + Arrays.toString(byteArrayField) +
                '}';
    }
}
