package com.delamater.wikipedia.aggregation;

import static com.mongodb.client.model.Filters.eq;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.delamater.wikipedia.aggregation.model.ScoredPage;
import com.delamater.wikipedia.aggregation.model.TopN;
import com.mongodb.client.model.ReplaceOptions;

import jakarta.annotation.PostConstruct;

/**
 * Publishes the live top-N to the single MongoDB doc {@code hot_topics/_id="current"} — the
 * doc the Phase 3 dashboard watches via a change stream.
 *
 * <p>With hopping windows several windows are live at once. We publish the <em>oldest
 * still-open</em> window (via {@link CurrentWindowTracker}), not the newest: the newest just
 * opened and is nearly empty, whereas the oldest open window holds a nearly-full trailing
 * window and still receives the latest edits. Publishing the newest made the dashboard reset
 * to ~0 every advance; publishing the oldest open one gives a smoothly-rolling trailing hour.
 */
@Component
public class HotTopicsWriter {

    private static final Logger log = LoggerFactory.getLogger(HotTopicsWriter.class);

    private final MongoTemplate mongo;

    @Value("${app.window.size-minutes}")
    private long windowSizeMinutes;

    private CurrentWindowTracker tracker;
    private TopN lastPublished;

    public HotTopicsWriter(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @PostConstruct
    void init() {
        this.tracker = new CurrentWindowTracker(Duration.ofMinutes(windowSizeMinutes).toMillis());
    }

    public synchronized void write(TopN top, int n) {
        TopN current = tracker.onUpdate(top);
        // Same reference ⇒ neither the selected window nor its contents changed → nothing to do.
        if (current == lastPublished) {
            return;
        }
        lastPublished = current;
        publish(current, n);
    }

    private void publish(TopN top, int n) {
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
