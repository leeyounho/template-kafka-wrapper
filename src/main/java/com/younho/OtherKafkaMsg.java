package com.younho;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public class OtherKafkaMsg {
    private Map<String, Object> otherValue = new HashMap<>();

    public void update(String key, Object value) {
        this.otherValue.put(key, value);
    }

    @JsonValue
    public Map<String, Object> getOtherValue() {
        return otherValue;
    }

    public void setOtherValue(Map<String, Object> otherValue) {
        this.otherValue = otherValue;
    }
}