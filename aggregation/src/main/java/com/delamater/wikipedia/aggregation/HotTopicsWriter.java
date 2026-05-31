package com.delamater.wikipedia.aggregation;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.delamater.wikipedia.aggregation.model.ScoredPage;
import com.delamater.wikipedia.aggregation.model.TopN;
import com.mongodb.client.model.ReplaceOptions;

/**
 * Publishes the live top-N to the single MongoDB doc {@code hot_topics/_id="current"} — the
 * doc the Phase 3 dashboard watches via a change stream.
 *
 * <p>With hopping windows several windows are live at once and updates can arrive
 * out-of-order across them. We only publish the window with the greatest {@code windowEnd}
 * seen, so {@code current} always reflects "now" and never regresses to an older window.
 */
@Component
public class HotTopicsWriter {

    private static final Logger log = LoggerFactory.getLogger(HotTopicsWriter.class);

    private final MongoTemplate mongo;
    private long maxWindowEnd = Long.MIN_VALUE;

    public HotTopicsWriter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    public synchronized void write(TopN top, int n) {
        if (top.getWindowEnd() < maxWindowEnd) {
            return; // stale update for an older overlapping window
        }
        maxWindowEnd = top.getWindowEnd();

        List<Document> topPages = new ArrayList<>();
        int rank = 1;
        for (ScoredPage p : top.topList(n)) {
            topPages.add(new Document()
                    .append("rank", rank++)
                    .append("title", p.title())
                    .append("pageUrl", p.pageUrl())
                    .append("editCount", p.editCount())
                    .append("distinctEditors", p.distinctEditors())
                    .append("bytesChanged", p.bytesChanged())
                    .append("hotnessScore", p.hotnessScore()));
        }

        Document doc = new Document("_id", "current")
                .append("windowStart", new Date(top.getWindowStart()))
                .append("windowEnd", new Date(top.getWindowEnd()))
                .append("generatedAt", new Date())
                .append("topPages", topPages);

        mongo.getCollection("hot_topics")
                .replaceOne(eq("_id", "current"), doc, new ReplaceOptions().upsert(true));

        if (log.isDebugEnabled()) {
            log.debug("hot_topics updated: window end={}, {} pages, leader='{}'",
                    top.getWindowEnd(), topPages.size(),
                    topPages.isEmpty() ? "-" : topPages.get(0).getString("title"));
        }
    }
}
