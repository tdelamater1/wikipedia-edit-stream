package com.delamater.wikipedia.aggregation.model;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * The single-key aggregate that realises the "top-N across keys" pattern: every page for a
 * given window is merged here so one task can rank them.
 *
 * <p>Keeps the latest {@link ScoredPage} per title (each carries the page's cumulative
 * window stats, so a later edit just overwrites the earlier entry) and bounds memory by
 * pruning the lowest-hotness page once it exceeds {@code maxTracked}. A pruned page that
 * keeps getting edits simply re-enters with its updated cumulative score, so top-N stays
 * correct as long as {@code maxTracked >= n}. A page that arrives with a non-positive score
 * is treated as excluded (a rollback) and removed — so a page that turns into a rollback
 * mid-window drops out instead of lingering with a stale score.
 */
public class TopN {

    private Map<String, ScoredPage> pages = new HashMap<>();
    private long windowStart;
    private long windowEnd;

    public TopN() {
    }

    public TopN merge(ScoredPage page, int maxTracked) {
        this.windowStart = page.windowStart();
        this.windowEnd = page.windowEnd();
        // A non-positive score marks an excluded page (rollback/self-revert): drop it if we
        // were tracking it and never add it. Legitimate pages always score > 0.
        if (page.hotnessScore() <= 0) {
            pages.remove(page.title());
            return this;
        }
        pages.put(page.title(), page);
        if (pages.size() > maxTracked) {
            pages.entrySet().stream()
                    .min(Comparator.comparingDouble(e -> e.getValue().hotnessScore()))
                    .map(Map.Entry::getKey)
                    .ifPresent(pages::remove);
        }
        return this;
    }

    /** Highest hotness first; title as a stable tie-breaker. */
    @JsonIgnore
    public List<ScoredPage> topList(int n) {
        return pages.values().stream()
                .sorted(Comparator.comparingDouble(ScoredPage::hotnessScore).reversed()
                        .thenComparing(ScoredPage::title))
                .limit(n)
                .toList();
    }

    public Map<String, ScoredPage> getPages() {
        return pages;
    }

    public void setPages(Map<String, ScoredPage> pages) {
        this.pages = (pages != null) ? pages : new HashMap<>();
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }
}
