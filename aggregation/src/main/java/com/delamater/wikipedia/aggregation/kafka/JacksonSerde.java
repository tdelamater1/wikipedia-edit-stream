package com.delamater.wikipedia.aggregation.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import tools.jackson.databind.ObjectMapper;

/**
 * A minimal JSON Serde backed by the Boot-provided Jackson 3 {@link ObjectMapper}. Used for
 * the aggregate types flowing through repartition topics / state stores. Rolling our own
 * avoids spring-kafka's JsonSerde type-header/trusted-package configuration.
 */
public class JacksonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper;
    private final Class<T> type;

    public JacksonSerde(ObjectMapper mapper, Class<T> type) {
        this.mapper = mapper;
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> (data == null) ? null : mapper.writeValueAsBytes(data);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> (bytes == null || bytes.length == 0) ? null : mapper.readValue(bytes, type);
    }
}
