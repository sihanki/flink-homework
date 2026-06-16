package com.example.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JoinedRecordSerializer implements Serializer<JoinedRecord> {
    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, JoinedRecord data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
