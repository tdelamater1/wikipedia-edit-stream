package com.delamater.wikipedia.aggregation;

import com.delamater.wikipedia.aggregation.model.PageAgg;

/**
 * Computes a page's hotness in "editor-units" (one distinct editor = 1 point):
 *
 * <pre>
 *   hotness = distinctEditors                         // linear, uncapped  — diversity dominates
 *           + 2.0 * ramp(editCount,      S)           // edit volume:  sub-linear, capped at 2
 *           + 1.0 * ramp(|bytesChanged|, B)           // byte change:  sub-linear, capped at 1
 * </pre>
 *
 * where {@code ramp(v, sat) = min(1, ln(1+v) / ln(1+sat))} is a 0..1 sub-linear curve that
 * reaches its cap at {@code v == sat}. Raw edits are deliberately sub-linearised and capped so
 * a single prolific editor (or a bot) can't outrank genuine breadth of participation — the most
 * a one-editor page can ever score is {@code 1 + 2 + 1 = 4}, i.e. four distinct editors' worth.
 *
 * <p>A net-zero byte change by at most two editors over two or more edits is treated as a
 * rollback / self-revert and scored {@link #EXCLUDED} so the top-N drops it.
 */
public class Hotness {

    /** Sentinel score for excluded pages; the top-N treats any score &lt;= 0 as "remove". */
    public static final double EXCLUDED = 0.0;

    private final int editSaturation;  // S: editCount at which the +2 edit bonus maxes out
    private final long byteSaturation; // B: |net bytes| at which the +1 byte bonus maxes out

    public Hotness(int editSaturation, long byteSaturation) {
        this.editSaturation = editSaturation;
        this.byteSaturation = byteSaturation;
    }

    public double score(PageAgg agg) {
        if (isRollback(agg)) {
            return EXCLUDED;
        }
        double editBonus = 2.0 * ramp(agg.getEditCount(), editSaturation);
        double byteBonus = 1.0 * ramp(Math.abs(agg.getBytesChanged()), byteSaturation);
        return agg.distinctEditors() + editBonus + byteBonus;
    }

    /** True when a net-zero byte change looks like an edit being reverted (or self-reverted). */
    public boolean isRollback(PageAgg agg) {
        return agg.getBytesChanged() == 0
                && agg.distinctEditors() <= 2
                && agg.getEditCount() >= 2;
    }

    /** Sub-linear 0..1 ramp; reaches 1 at {@code value == saturation}, capped above it. */
    private static double ramp(long value, long saturation) {
        if (value <= 0) {
            return 0.0;
        }
        double denom = Math.log1p(saturation);
        if (denom <= 0.0) {
            return 1.0; // saturation <= 0: any activity maxes the bonus
        }
        return Math.min(1.0, Math.log1p(value) / denom);
    }
}
