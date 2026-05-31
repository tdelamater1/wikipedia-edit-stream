package com.delamater.wikipedia.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.delamater.wikipedia.aggregation.model.PageAgg;

/** Pure unit tests for the hotness scoring + rollback exclusion. No Kafka/Mongo. */
class HotnessTest {

    // Defaults: edit bonus maxes at 10 edits, byte bonus maxes at 2000 net bytes.
    private final Hotness hotness = new Hotness(10, 2000);

    private static PageAgg agg(long editCount, int distinctEditors, long bytesChanged) {
        PageAgg a = new PageAgg();
        a.setEditCount(editCount);
        a.setBytesChanged(bytesChanged);
        Set<String> editors = new HashSet<>();
        for (int i = 0; i < distinctEditors; i++) {
            editors.add("editor-" + i);
        }
        a.setEditors(editors);
        return a;
    }

    @Test
    void classicRollbackIsExcluded() {
        // two editors, two edits, net-zero bytes -> an edit reverted
        assertThat(hotness.score(agg(2, 2, 0))).isEqualTo(Hotness.EXCLUDED);
    }

    @Test
    void selfRevertIsExcluded() {
        // one editor, two edits, net-zero bytes -> editor undid their own edit
        assertThat(hotness.score(agg(2, 1, 0))).isEqualTo(Hotness.EXCLUDED);
    }

    @Test
    void singleNetZeroEditIsNotExcluded() {
        // one edit that happens to be net-zero (e.g. a same-length typo fix) is legitimate
        assertThat(hotness.score(agg(1, 1, 0))).isGreaterThan(0.0);
    }

    @Test
    void netZeroWithThreeEditorsIsNotExcluded() {
        // 3+ editors netting to zero is treated as genuine activity, not a rollback
        assertThat(hotness.score(agg(3, 3, 0))).isGreaterThan(0.0);
    }

    @Test
    void distinctEditorsBeatSameEditorVolume() {
        double sameEditor = hotness.score(agg(3, 1, 300)); // 3 edits, 1 editor
        double distinct = hotness.score(agg(2, 2, 400));   // 2 edits, 2 distinct editors
        assertThat(distinct).isGreaterThan(sameEditor);
    }

    @Test
    void singleEditorCeilingIsFour() {
        // The most a one-editor page can score: 1 (self) + 2 (edit cap) + 1 (byte cap).
        assertThat(hotness.score(agg(1000, 1, 50_000))).isCloseTo(4.0, within(1e-9));
    }

    @Test
    void diversityAlwaysBeatsTheSingleEditorCeiling() {
        double ceiling = hotness.score(agg(1000, 1, 50_000)); // 4.0
        double fiveEditors = hotness.score(agg(5, 5, 100));    // >= 5
        assertThat(fiveEditors).isGreaterThan(ceiling);
    }

    @Test
    void editBonusIsCappedAtTwoEditorUnits() {
        double atCap = hotness.score(agg(10, 3, 100));
        double wayPastCap = hotness.score(agg(10_000, 3, 100));
        assertThat(wayPastCap).isCloseTo(atCap, within(1e-9));
    }

    @Test
    void byteBonusIsCappedAtOneEditorUnit() {
        double atCap = hotness.score(agg(3, 3, 2000));
        double wayPastCap = hotness.score(agg(3, 3, 500_000));
        assertThat(wayPastCap).isCloseTo(atCap, within(1e-9));
    }

    @Test
    void byteBonusUsesAbsoluteValue() {
        // a net removal counts the same as a net addition of equal magnitude
        assertThat(hotness.score(agg(3, 2, -300))).isEqualTo(hotness.score(agg(3, 2, 300)));
    }

    @Test
    void moreDistinctEditorsScoresHigher() {
        assertThat(hotness.score(agg(3, 3, 100))).isGreaterThan(hotness.score(agg(3, 2, 100)));
    }
}
