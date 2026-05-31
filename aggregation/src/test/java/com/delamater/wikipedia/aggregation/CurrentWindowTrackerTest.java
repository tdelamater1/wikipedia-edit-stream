package com.delamater.wikipedia.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.delamater.wikipedia.aggregation.model.TopN;

/** Unit tests for the oldest-open-window selection. No Kafka/Mongo. */
class CurrentWindowTrackerTest {

    private static final long MIN = 60_000L;
    private static final long SIZE = 60 * MIN; // 60-minute window

    /** A TopN standing in for one hopping-window instance ending at {@code endMin} minutes. */
    private static TopN window(long endMin) {
        TopN t = new TopN();
        long end = endMin * MIN;
        t.setWindowEnd(end);
        t.setWindowStart(end - SIZE);
        return t;
    }

    /** Feed the 12 windows open when stream-time ≈ 60 min (ends 65..120 by 5). */
    private static TopN feedOpenWindows(CurrentWindowTracker tracker) {
        TopN last = null;
        for (long endMin = 65; endMin <= 120; endMin += 5) {
            last = tracker.onUpdate(window(endMin));
        }
        return last;
    }

    @Test
    void selectsOldestOpenWindowNotNewest() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        TopN selected = feedOpenWindows(tracker);
        // stream-time ≈ 120 - 60 = 60; oldest open = smallest end > 60 = 65 (NOT the newest, 120).
        assertThat(selected.getWindowEnd()).isEqualTo(65 * MIN);
    }

    @Test
    void youngestWindowAloneSelectedAtStartup() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        TopN selected = tracker.onUpdate(window(100));
        assertThat(selected.getWindowEnd()).isEqualTo(100 * MIN);
    }

    @Test
    void advancesByOneAndDropsClosedWindow() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        feedOpenWindows(tracker);
        // A new window opens (end 125 → stream-time 65); the 65 window has now closed.
        TopN selected = tracker.onUpdate(window(125));
        assertThat(selected.getWindowEnd()).isEqualTo(70 * MIN);
    }

    @Test
    void updateToOpenTargetReturnsRefreshedObject() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        feedOpenWindows(tracker);
        TopN refreshed = window(65); // an edit lands in the displayed (oldest-open) window
        TopN selected = tracker.onUpdate(refreshed);
        assertThat(selected).isSameAs(refreshed); // same ref ⇒ caller republishes
    }

    @Test
    void updateToNonTargetWindowReturnsSameReference() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        feedOpenWindows(tracker);
        TopN target = tracker.onUpdate(window(65)); // pin the current target object
        TopN afterOther = tracker.onUpdate(window(110)); // a younger window updates
        // Target unchanged → same reference returned ⇒ caller skips the redundant write.
        assertThat(afterOther).isSameAs(target);
        assertThat(afterOther.getWindowEnd()).isEqualTo(65 * MIN);
    }

    @Test
    void lateUpdateToClosedWindowIsIgnored() {
        CurrentWindowTracker tracker = new CurrentWindowTracker(SIZE);
        feedOpenWindows(tracker);
        tracker.onUpdate(window(125)); // target advances to 70; 65 closed
        TopN selected = tracker.onUpdate(window(65)); // a late edit for the closed window
        assertThat(selected.getWindowEnd()).isEqualTo(70 * MIN); // still 70, 65 stays dropped
    }
}
