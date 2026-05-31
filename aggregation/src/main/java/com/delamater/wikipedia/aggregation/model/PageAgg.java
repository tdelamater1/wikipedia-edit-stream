package com.delamater.wikipedia.aggregation.model;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Per-page accumulator for one window. Mutable JavaBean so Kafka Streams can fold edits into
 * it and the JSON serde can round-trip it through the state store / changelog.
 *
 * <p>{@code editors} is a Set so re-counting the same user doesn't inflate distinct-editor
 * counts (the basis for the hotness weighting).
 */
public class PageAgg {

    private String title;
    private String domain;
    private long editCount;
    private Set<String> editors = new HashSet<>();
    private long bytesChanged;

    public PageAgg() {
    }

    /** Fold one edit into this aggregate. */
    public PageAgg add(WikipediaEdit edit) {
        this.title = edit.title();
        this.domain = edit.domain();
        this.editCount++;
        if (edit.userName() != null && !edit.userName().isBlank()) {
            this.editors.add(edit.userName());
        }
        this.bytesChanged += edit.byteDelta();
        return this;
    }

    @JsonIgnore
    public int distinctEditors() {
        return editors.size();
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public long getEditCount() {
        return editCount;
    }

    public void setEditCount(long editCount) {
        this.editCount = editCount;
    }

    public Set<String> getEditors() {
        return editors;
    }

    public void setEditors(Set<String> editors) {
        this.editors = (editors != null) ? editors : new HashSet<>();
    }

    public long getBytesChanged() {
        return bytesChanged;
    }

    public void setBytesChanged(long bytesChanged) {
        this.bytesChanged = bytesChanged;
    }
}
