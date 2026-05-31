package com.delamater.wikipedia.aggregation;

/** Reconstructs the canonical article URL (spaces → underscores; namespace prefix kept). */
public final class PageUrls {

    private PageUrls() {
    }

    public static String of(String domain, String title) {
        if (domain == null || title == null || title.isBlank()) {
            return null;
        }
        return "https://" + domain + "/wiki/" + title.strip().replace(' ', '_');
    }
}
