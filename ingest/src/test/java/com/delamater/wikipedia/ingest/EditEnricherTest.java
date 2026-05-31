package com.delamater.wikipedia.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.delamater.wikipedia.ingest.model.EditDocument;
import com.delamater.wikipedia.ingest.model.WikipediaEdit;

/**
 * Plain unit tests for the enrichment logic — no Spring context, so they run
 * without a live Kafka or MongoDB.
 */
class EditEnricherTest {

    private final EditEnricher enricher = new EditEnricher();

    @Test
    void reconstructsPageUrlWithUnderscores() {
        assertThat(EditEnricher.pageUrl("en.wikipedia.org", "Albert Einstein"))
                .isEqualTo("https://en.wikipedia.org/wiki/Albert_Einstein");
    }

    @Test
    void keepsNamespacePrefixInPageUrl() {
        assertThat(EditEnricher.pageUrl("en.wikipedia.org", "Talk:Main Page"))
                .isEqualTo("https://en.wikipedia.org/wiki/Talk:Main_Page");
    }

    @Test
    void pageUrlNullWhenTitleBlank() {
        assertThat(EditEnricher.pageUrl("en.wikipedia.org", "  ")).isNull();
    }

    @Test
    void computesByteDelta() {
        assertThat(EditEnricher.byteDelta(100, 250)).isEqualTo(150);
        assertThat(EditEnricher.byteDelta(250, 100)).isEqualTo(-150);
    }

    @Test
    void byteDeltaNullWhenLengthMissing() {
        assertThat(EditEnricher.byteDelta(null, 250)).isNull();
        assertThat(EditEnricher.byteDelta(100, null)).isNull();
    }

    @Test
    void parsesIsoTimestampAndNullsGarbage() {
        assertThat(EditEnricher.parseTimestamp("2026-05-30T12:34:56Z"))
                .isEqualTo(Instant.parse("2026-05-30T12:34:56Z"));
        assertThat(EditEnricher.parseTimestamp("not-a-date")).isNull();
        assertThat(EditEnricher.parseTimestamp(null)).isNull();
    }

    @Test
    void mapsFullEditToDocument() {
        WikipediaEdit edit = new WikipediaEdit(
                123456789L,
                "en.wikipedia.org",
                "main namespace",
                "Albert Einstein",
                "fixed a typo",
                "2026-05-30T12:34:56Z",
                "SomeEditor",
                false,
                1000,
                1042,
                987L,
                988L);

        EditDocument doc = enricher.toDocument(edit);

        assertThat(doc.id()).isEqualTo("123456789");
        assertThat(doc.pageUrl()).isEqualTo("https://en.wikipedia.org/wiki/Albert_Einstein");
        assertThat(doc.user()).isEqualTo("SomeEditor");
        assertThat(doc.byteDelta()).isEqualTo(42);
        assertThat(doc.timestamp()).isEqualTo(Instant.parse("2026-05-30T12:34:56Z"));
        assertThat(doc.ingestedAt()).isNotNull();
    }
}
