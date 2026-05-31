package com.delamater.wikipedia.aggregation.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The fields of a `wikipedia-events` message that the aggregation needs. (Annotations stay
 * under com.fasterxml.jackson.annotation even on Jackson 3.)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WikipediaEdit(
        String domain,
        String namespace,
        String title,
        @JsonProperty("user_name") String userName,
        @JsonProperty("old_length") Integer oldLength,
        @JsonProperty("new_length") Integer newLength
) {
    /** Net byte change, 0 when either length is missing. */
    public int byteDelta() {
        return (oldLength == null || newLength == null) ? 0 : newLength - oldLength;
    }
}
