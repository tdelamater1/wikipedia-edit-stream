package com.delamater.wikipedia.ingest;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.delamater.wikipedia.ingest.model.EditDocument;
import com.delamater.wikipedia.ingest.model.WikipediaEdit;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes raw edit JSON from {@code wikipedia-events}, enriches it, and upserts
 * it into the {@code edits} collection.
 *
 * <p>Delivery is at-least-once: the listener container commits offsets only after
 * this method returns normally. A malformed message is logged and skipped (so one
 * bad record can't wedge the partition); any other failure propagates to Spring
 * Kafka's error handler for retry. Idempotent upserts (id = source event id) make
 * the at-least-once retries safe.
 */
@Component
public class EditConsumer {

    private static final Logger log = LoggerFactory.getLogger(EditConsumer.class);

    private final ObjectMapper objectMapper;
    private final EditEnricher enricher;
    private final EditRepository repository;
    private final AtomicLong saved = new AtomicLong();

    public EditConsumer(ObjectMapper objectMapper, EditEnricher enricher, EditRepository repository) {
        this.objectMapper = objectMapper;
        this.enricher = enricher;
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String payload) {
        WikipediaEdit edit;
        try {
            edit = objectMapper.readValue(payload, WikipediaEdit.class);
        } catch (JacksonException ex) {
            log.warn("Skipping malformed message: {}", ex.getMessage());
            return;
        }

        EditDocument doc = enricher.toDocument(edit);
        repository.save(doc);

        long n = saved.incrementAndGet();
        if (n % 100 == 0) {
            log.info("Persisted {} edits (latest: '{}')", n, doc.title());
        }
    }
}
