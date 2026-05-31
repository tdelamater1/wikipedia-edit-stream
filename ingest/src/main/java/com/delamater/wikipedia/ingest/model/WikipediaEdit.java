package com.delamater.wikipedia.ingest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One edit event as published to the {@code wikipedia-events} Kafka topic by the
 * Python producer. Field names mirror the producer's snake_case JSON; unknown
 * fields are ignored so the producer can evolve without breaking ingest.
 *
 * <p>{@code old_length}/{@code new_length}/{@code revision_*} are nullable in the
 * source (some events omit them), so they are boxed types.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikipediaEdit(
        long id,
        String domain,
        String namespace,
        String title,
        String comment,
        String timestamp,
        @JsonProperty("user_name") String userName,
        boolean minor,
        @JsonProperty("old_length") Integer oldLength,
        @JsonProperty("new_length") Integer newLength,
        @JsonProperty("revision_old") Long revisionOld,
        @JsonProperty("revision_new") Long revisionNew
) {
}
