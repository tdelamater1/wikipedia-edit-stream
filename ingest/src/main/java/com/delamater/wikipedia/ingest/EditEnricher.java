package com.delamater.wikipedia.ingest;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.delamater.wikipedia.ingest.model.EditDocument;
import com.delamater.wikipedia.ingest.model.WikipediaEdit;

/**
 * Pure transformation from a raw {@link WikipediaEdit} to the enriched
 * {@link EditDocument} we persist. No I/O, so it is trivially unit-testable.
 */
@Component
public class EditEnricher {

    private static final Logger log = LoggerFactory.getLogger(EditEnricher.class);

    public EditDocument toDocument(WikipediaEdit e) {
        return new EditDocument(
                String.valueOf(e.id()),
                e.title(),
                pageUrl(e.domain(), e.title()),
                e.domain(),
                e.namespace(),
                e.userName(),
                e.comment(),
                e.minor(),
                e.oldLength(),
                e.newLength(),
                byteDelta(e.oldLength(), e.newLength()),
                e.revisionOld(),
                e.revisionNew(),
                parseTimestamp(e.timestamp()),
                Instant.now());
    }

    /**
     * Reconstruct the canonical article URL. Wikipedia article paths replace
     * spaces with underscores; the title may carry a namespace prefix
     * (e.g. {@code "Talk:Foo"}), which the {@code /wiki/} path handles as-is.
     * Full percent-encoding of exotic characters is left as a later refinement;
     * underscores keep the URL human-readable in Compass.
     */
    static String pageUrl(String domain, String title) {
        if (domain == null || title == null || title.isBlank()) {
            return null;
        }
        return "https://" + domain + "/wiki/" + title.strip().replace(' ', '_');
    }

    /** Net byte change of the edit, or null if either length is missing. */
    static Integer byteDelta(Integer oldLength, Integer newLength) {
        if (oldLength == null || newLength == null) {
            return null;
        }
        return newLength - oldLength;
    }

    /** Parse the ISO-8601 event time (from meta.dt). Null on absent/garbage. */
    static Instant parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException ex) {
            log.warn("Unparseable timestamp '{}': {}", timestamp, ex.getMessage());
            return null;
        }
    }
}
