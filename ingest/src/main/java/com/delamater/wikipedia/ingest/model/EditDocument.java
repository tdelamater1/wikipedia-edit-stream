package com.delamater.wikipedia.ingest.model;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * The enriched edit as stored in the {@code edits} collection.
 *
 * <p>The Mongo {@code _id} is the source event id (as a String). Using a stable,
 * source-derived id makes re-processing idempotent: a replayed message simply
 * upserts the same document instead of creating a duplicate (at-least-once
 * delivery is fine without a dedupe table).
 */
@Document(collection = "edits")
public record EditDocument(
        @Id String id,
        String title,
        String pageUrl,
        String domain,
        String namespace,
        String user,
        String comment,
        boolean minor,
        Integer oldLength,
        Integer newLength,
        Integer byteDelta,
        Long revisionOld,
        Long revisionNew,
        Instant timestamp,
        Instant ingestedAt
) {
}
