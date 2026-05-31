package com.delamater.wikipedia.ingest;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.delamater.wikipedia.ingest.model.EditDocument;

/**
 * Spring Data repository for the {@code edits} collection. {@code save()} upserts
 * by {@code _id} (the source event id), giving idempotent writes on replay.
 */
public interface EditRepository extends MongoRepository<EditDocument, String> {
}
