package com.delamater.wikipedia.aggregation;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import com.delamater.wikipedia.aggregation.kafka.JacksonSerde;
import com.delamater.wikipedia.aggregation.model.PageAgg;
import com.delamater.wikipedia.aggregation.model.ScoredPage;
import com.delamater.wikipedia.aggregation.model.TopN;
import com.delamater.wikipedia.aggregation.model.WikipediaEdit;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Kafka Streams topology: {@code wikipedia-events} → windowed per-page aggregate → top-N →
 * MongoDB {@code hot_topics}.
 *
 * <pre>
 * raw JSON ──parse/filter──▶ key=title ──hopping window──▶ KTable&lt;(title,window), PageAgg&gt;
 *   ──map to (windowEnd, ScoredPage)──▶ groupByKey(windowEnd) ──▶ KTable&lt;windowEnd, TopN&gt;
 *   ──foreach──▶ HotTopicsWriter (upsert hot_topics/_id=current)
 * </pre>
 *
 * The re-key to a single {@code windowEnd} is the "top-N across keys" collapse: it funnels
 * every page for a window into one task so it can be ranked.
 */
@Configuration
@EnableKafkaStreams
public class AggregationTopology {

    private static final Logger log = LoggerFactory.getLogger(AggregationTopology.class);
    private static final String MAIN_NAMESPACE = "main namespace";

    private final ObjectMapper mapper;
    private final HotTopicsWriter writer;

    @Value("${app.kafka.topic}")
    private String topic;
    @Value("${app.window.size-minutes}")
    private long windowSizeMinutes;
    @Value("${app.window.advance-minutes}")
    private long windowAdvanceMinutes;
    @Value("${app.window.grace-minutes}")
    private long windowGraceMinutes;
    @Value("${app.topn.size}")
    private int topnSize;
    @Value("${app.topn.max-tracked}")
    private int maxTracked;
    @Value("${app.filter.main-namespace-only}")
    private boolean mainNamespaceOnly;
    @Value("${app.hotness.edit-saturation}")
    private int editSaturation;
    @Value("${app.hotness.byte-saturation}")
    private long byteSaturation;

    public AggregationTopology(ObjectMapper mapper, HotTopicsWriter writer) {
        this.mapper = mapper;
        this.writer = writer;
    }

    @Bean
    public KStream<String, String> hotTopicsPipeline(StreamsBuilder builder) {
        JacksonSerde<WikipediaEdit> editSerde = new JacksonSerde<>(mapper, WikipediaEdit.class);
        JacksonSerde<PageAgg> pageAggSerde = new JacksonSerde<>(mapper, PageAgg.class);
        JacksonSerde<ScoredPage> scoredSerde = new JacksonSerde<>(mapper, ScoredPage.class);
        JacksonSerde<TopN> topNSerde = new JacksonSerde<>(mapper, TopN.class);

        Hotness hotness = new Hotness(editSaturation, byteSaturation);

        TimeWindows windows = TimeWindows
                .ofSizeAndGrace(Duration.ofMinutes(windowSizeMinutes), Duration.ofMinutes(windowGraceMinutes))
                .advanceBy(Duration.ofMinutes(windowAdvanceMinutes));

        KStream<String, String> raw = builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));

        // Parse + filter, then re-key by page title.
        KStream<String, WikipediaEdit> byTitle = raw
                .mapValues(this::parse)
                .filter((k, v) -> v != null && v.title() != null && !v.title().isBlank())
                .filter((k, v) -> !mainNamespaceOnly || MAIN_NAMESPACE.equals(v.namespace()))
                .selectKey((k, v) -> v.title());

        // Per-page windowed aggregate (RocksDB-backed window store).
        KTable<Windowed<String>, PageAgg> perPage = byTitle
                .groupByKey(Grouped.with(Serdes.String(), editSerde))
                .windowedBy(windows)
                .aggregate(
                        PageAgg::new,
                        (title, edit, agg) -> agg.add(edit),
                        Materialized.<String, PageAgg, WindowStore<Bytes, byte[]>>as("page-windowed-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(pageAggSerde));

        // Collapse to a single key (windowEnd) and rank.
        // note: does a pivot on the key
        // Excluded pages (rollbacks) are scored Hotness.EXCLUDED and pass through so the
        // top-N can drop them — important when a page *becomes* a rollback mid-window.
        KStream<Long, ScoredPage> scored = perPage.toStream()
                .map((windowedKey, agg) -> {
                    long start = windowedKey.window().start();
                    long end = windowedKey.window().end();
                    String title = windowedKey.key();
                    double score = hotness.score(agg);
                    if (score <= Hotness.EXCLUDED && log.isDebugEnabled()) {
                        log.debug("excluding rollback: title='{}', edits={}, editors={}, netBytes={}",
                                title, agg.getEditCount(), agg.distinctEditors(), agg.getBytesChanged());
                    }
                    ScoredPage page = new ScoredPage(
                            title, PageUrls.of(agg.getDomain(), title),
                            agg.getEditCount(), agg.distinctEditors(), agg.getBytesChanged(),
                            score, start, end);
                    return KeyValue.pair(end, page);
                });

        KTable<Long, TopN> topN = scored
                .groupByKey(Grouped.with(Serdes.Long(), scoredSerde))
                .aggregate(
                        TopN::new,
                        (windowEnd, page, agg) -> agg.merge(page, maxTracked),
                        Materialized.<Long, TopN, KeyValueStore<Bytes, byte[]>>as("topn-by-window")
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(topNSerde));

        topN.toStream().foreach((windowEnd, top) -> writer.write(top, topnSize));

        log.info("Topology built: topic='{}', window={}m/advance={}m/grace={}m, topN={}, mainNsOnly={}, editSat={}, byteSat={}",
                topic, windowSizeMinutes, windowAdvanceMinutes, windowGraceMinutes, topnSize, mainNamespaceOnly,
                editSaturation, byteSaturation);
        return raw;
    }

    /** Parse a message to WikipediaEdit; null (dropped downstream) on malformed JSON. */
    private WikipediaEdit parse(String json) {
        try {
            return mapper.readValue(json, WikipediaEdit.class);
        } catch (JacksonException ex) {
            log.warn("Skipping malformed message: {}", ex.getMessage());
            return null;
        }
    }
}
