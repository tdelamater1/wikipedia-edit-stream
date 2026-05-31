package com.delamater.wikipedia.aggregation.model;

/**
 * A page's windowed stats plus its computed hotness, carrying the window bounds so the
 * downstream top-N (keyed only by windowEnd) and the Mongo writer have everything they need.
 */
public record ScoredPage(
        String title,
        String pageUrl,
        long editCount,
        int distinctEditors,
        long bytesChanged,
        double hotnessScore,
        long windowStart,
        long windowEnd
) {
}
