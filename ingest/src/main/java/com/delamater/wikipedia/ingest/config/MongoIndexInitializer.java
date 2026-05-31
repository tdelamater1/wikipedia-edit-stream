package com.delamater.wikipedia.ingest.config;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.stereotype.Component;

import com.delamater.wikipedia.ingest.model.EditDocument;

/**
 * Creates the indexes the {@code edits} collection needs, on startup. Done
 * explicitly (rather than via {@code spring.data.mongodb.auto-index-creation})
 * so the index intent is visible in code and the TTL is property-driven.
 *
 * <ul>
 *   <li>{@code {title:1, timestamp:-1}} — per-page history lookups (Phase 2+).</li>
 *   <li>TTL {@code {ingestedAt:1}} — expires raw edits after the configured
 *       window so the collection never grows unbounded.</li>
 * </ul>
 */
@Component
public class MongoIndexInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MongoIndexInitializer.class);

    private final MongoTemplate mongoTemplate;
    private final long ttlSeconds;
    private final String username;
    private final String password;

    public MongoIndexInitializer(MongoTemplate mongoTemplate,
                                 @Value("${app.edits.ttl-seconds}") long ttlSeconds,
                                 @Value("${spring.mongodb.username:}") String username,
                                 @Value("${spring.mongodb.password:}") String password) {
        this.mongoTemplate = mongoTemplate;
        this.ttlSeconds = ttlSeconds;
        this.username = username;
        this.password = password;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Diagnostic only (never logs the password itself): if passwordLength=0 the
        // credential won't be built and Mongo will reject with code 13 (Unauthorized).
        log.info("Mongo auth config -> username='{}', passwordLength={}",
                username, password == null ? 0 : password.length());

        IndexOperations ops = mongoTemplate.indexOps(EditDocument.class);

        ops.ensureIndex(new Index()
                .on("title", Sort.Direction.ASC)
                .on("timestamp", Sort.Direction.DESC)
                .named("title_timestamp"));

        ops.ensureIndex(new Index()
                .on("ingestedAt", Sort.Direction.ASC)
                .expire(Duration.ofSeconds(ttlSeconds))
                .named("ingestedAt_ttl"));

        log.info("Ensured indexes on 'edits' (TTL {}s on ingestedAt)", ttlSeconds);
    }
}
