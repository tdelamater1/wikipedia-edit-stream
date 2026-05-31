package com.delamater.wikipedia.aggregation;

import java.util.NavigableMap;
import java.util.TreeMap;

import com.delamater.wikipedia.aggregation.model.TopN;

/**
 * Picks which hopping-window instance the dashboard should display.
 *
 * <p>Several windows are open at once. The one with the <em>greatest</em> {@code windowEnd}
 * just opened and is nearly empty, so publishing it makes the dashboard look like it resets to
 * zero every advance (showing only minutes of a 60-minute window). Instead we publish the
 * <em>oldest still-open</em> window: it holds a nearly-full window of data <em>and</em> is
 * still receiving the latest edits, so the view is a smoothly-rolling trailing window rather
 * than a sawtooth.
 *
 * <p>Stream-time is estimated as {@code maxWindowEnd - windowSize} (≈ the start of the youngest
 * window). The oldest open window is the smallest {@code windowEnd} still greater than that;
 * everything at or below it has closed and is pruned.
 *
 * <p>{@link #onUpdate} returns the latest {@link TopN} for the selected window every call,
 * returning the <em>same object reference</em> until that window's contents change or the
 * selection advances — letting the caller skip redundant writes with a cheap {@code ==} check.
 */
class CurrentWindowTracker {

    private final long windowSizeMs;
    private final NavigableMap<Long, TopN> byWindowEnd = new TreeMap<>();
    private long maxWindowEnd = Long.MIN_VALUE;

    CurrentWindowTracker(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    synchronized TopN onUpdate(TopN top) {
        long end = top.getWindowEnd();
        byWindowEnd.put(end, top);
        if (end > maxWindowEnd) {
            maxWindowEnd = end;
        }
        long streamTime = maxWindowEnd - windowSizeMs; // ≈ start of the youngest window
        byWindowEnd.headMap(streamTime, true).clear();  // drop windows that have closed
        return byWindowEnd.firstEntry().getValue();     // oldest still-open window
    }
}
