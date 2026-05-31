package com.delamater.wikipedia.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.delamater.wikipedia.aggregation.model.ScoredPage;
import com.delamater.wikipedia.aggregation.model.TopN;

/** Pure unit tests for the top-N ranking + bounded-size pruning. No Kafka/Mongo. */
class TopNTest {

    private static ScoredPage page(String title, double hotness) {
        return new ScoredPage(title, "https://en.wikipedia.org/wiki/" + title,
                (long) hotness, 1, 0L, hotness, 0L, 600_000L);
    }

    @Test
    void ranksByHotnessDescending() {
        TopN top = new TopN();
        top.merge(page("Low", 1.0), 100);
        top.merge(page("High", 9.0), 100);
        top.merge(page("Mid", 5.0), 100);

        List<ScoredPage> ranked = top.topList(10);

        assertThat(ranked).extracting(ScoredPage::title).containsExactly("High", "Mid", "Low");
    }

    @Test
    void limitsToN() {
        TopN top = new TopN();
        for (int i = 0; i < 20; i++) {
            top.merge(page("p" + i, i), 100);
        }
        assertThat(top.topList(10)).hasSize(10);
        assertThat(top.topList(10).get(0).title()).isEqualTo("p19");
    }

    @Test
    void laterMergeOverwritesSameTitle() {
        TopN top = new TopN();
        top.merge(page("Page", 2.0), 100);
        top.merge(page("Page", 8.0), 100);

        assertThat(top.getPages()).hasSize(1);
        assertThat(top.topList(10).get(0).hotnessScore()).isEqualTo(8.0);
    }

    @Test
    void prunesLowestWhenExceedingMaxTracked() {
        TopN top = new TopN();
        // maxTracked=3: keep only the 3 highest-hotness pages.
        top.merge(page("a", 1.0), 3);
        top.merge(page("b", 2.0), 3);
        top.merge(page("c", 3.0), 3);
        top.merge(page("d", 4.0), 3); // pushes out "a" (lowest)

        assertThat(top.getPages()).hasSize(3);
        assertThat(top.getPages().keySet()).containsExactlyInAnyOrder("b", "c", "d");
        assertThat(top.topList(3)).extracting(ScoredPage::title).containsExactly("d", "c", "b");
    }

    @Test
    void carriesLatestWindowBounds() {
        TopN top = new TopN();
        top.merge(new ScoredPage("x", null, 1, 1, 0, 1.0, 100L, 200L), 10);
        assertThat(top.getWindowStart()).isEqualTo(100L);
        assertThat(top.getWindowEnd()).isEqualTo(200L);
    }
}
