package fi.hsl.transitdata.monitoring.gtfsrt;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GtfsRtMetricsRegistryTest {

    private MeterRegistry meterRegistry;
    private GtfsRtMetricsRegistry registry;
    private static final String TEST_URL = "http://example.com/gtfs-rt";
    private static final String TEST_URL_2 = "http://example.com/gtfs-rt-2";

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    void shouldRegisterEntityCountHistogramForEachUrl() {
        // given
        var urls = List.of(TEST_URL, TEST_URL_2);

        // when
        registry = new GtfsRtMetricsRegistry(meterRegistry, urls);

        // then
        var summary1 = meterRegistry.find("gtfsrt_entity_count").tag("url", TEST_URL).summary();
        var summary2 = meterRegistry.find("gtfsrt_entity_count").tag("url", TEST_URL_2).summary();

        assertThat(summary1).isNotNull();
        assertThat(summary2).isNotNull();
    }

    @Test
    void shouldRegisterTimestampAgeHistogramForEachUrl() {
        // given
        var urls = List.of(TEST_URL);

        // when
        registry = new GtfsRtMetricsRegistry(meterRegistry, urls);

        // then
        var summary = meterRegistry.find("gtfsrt_timestamp_age_seconds").tag("url", TEST_URL).summary();

        assertThat(summary).isNotNull();
        assertThat(summary.getId().getBaseUnit()).isEqualTo("seconds");
    }

    @Test
    void shouldRegisterLastScrapeSuccessGaugeForEachUrl() {
        // given
        var urls = List.of(TEST_URL);

        // when
        registry = new GtfsRtMetricsRegistry(meterRegistry, urls);

        // then
        var gauge = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(0.0);
    }

    @Test
    void shouldRecordEntityCountWhenSuccessfulScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));
        var entityCount = 42;

        // when
        registry.recordSuccessfulScrape(TEST_URL, entityCount, 10);

        // then
        var summary = meterRegistry.find("gtfsrt_entity_count").tag("url", TEST_URL).summary();

        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.totalAmount()).isEqualTo(entityCount);
    }

    @Test
    void shouldRecordTimestampAgeWhenSuccessfulScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));
        var timestampAge = 15;

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, timestampAge);

        // then
        var summary = meterRegistry.find("gtfsrt_timestamp_age_seconds").tag("url", TEST_URL).summary();

        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.totalAmount()).isEqualTo(timestampAge);
    }

    @Test
    void shouldSetLastScrapeSuccessToOneWhenSuccessfulScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, 10);

        // then
        var gauge = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(gauge.value()).isEqualTo(1.0);
    }

    @Test
    void shouldIncrementCounterWithSuccessResultWhenSuccessfulScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, 10);

        // then
        var counter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "success")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldSetLastScrapeSuccessToZeroWhenFailedScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));
        registry.recordSuccessfulScrape(TEST_URL, 100, 10); // first set to 1

        // when
        registry.recordFailedScrape(TEST_URL, "http_500");

        // then
        var gauge = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(gauge.value()).isEqualTo(0.0);
    }

    @Test
    void shouldIncrementCounterWithHttpErrorResultWhenFailedScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordFailedScrape(TEST_URL, "http_404");

        // then
        var counter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "http_404")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldIncrementCounterWithParseErrorResultWhenFailedScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordFailedScrape(TEST_URL, "parse_error");

        // then
        var counter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "parse_error")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldIncrementCounterWithIoErrorResultWhenFailedScrape() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordFailedScrape(TEST_URL, "io_error");

        // then
        var counter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "io_error")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldRecordMultipleSuccessfulScrapesInHistogram() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, 5);
        registry.recordSuccessfulScrape(TEST_URL, 150, 10);
        registry.recordSuccessfulScrape(TEST_URL, 120, 8);

        // then
        var entityCountSummary = meterRegistry.find("gtfsrt_entity_count").tag("url", TEST_URL).summary();
        var timestampAgeSummary = meterRegistry.find("gtfsrt_timestamp_age_seconds").tag("url", TEST_URL).summary();

        assertThat(entityCountSummary.count()).isEqualTo(3);
        assertThat(entityCountSummary.totalAmount()).isEqualTo(370); // 100 + 150 + 120
        assertThat(entityCountSummary.max()).isEqualTo(150);

        assertThat(timestampAgeSummary.count()).isEqualTo(3);
        assertThat(timestampAgeSummary.totalAmount()).isEqualTo(23); // 5 + 10 + 8
        assertThat(timestampAgeSummary.max()).isEqualTo(10);
    }

    @Test
    void shouldTrackSuccessAndFailureCountsSeparately() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL));

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, 10);
        registry.recordSuccessfulScrape(TEST_URL, 100, 10);
        registry.recordFailedScrape(TEST_URL, "http_500");
        registry.recordFailedScrape(TEST_URL, "io_error");

        // then
        var successCounter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "success")
                .counter();

        var http500Counter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "http_500")
                .counter();

        var ioErrorCounter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "io_error")
                .counter();

        assertThat(successCounter.count()).isEqualTo(2.0);
        assertThat(http500Counter.count()).isEqualTo(1.0);
        assertThat(ioErrorCounter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldHandleMultipleUrlsIndependently() {
        // given
        registry = new GtfsRtMetricsRegistry(meterRegistry, List.of(TEST_URL, TEST_URL_2));

        // when
        registry.recordSuccessfulScrape(TEST_URL, 100, 10);
        registry.recordFailedScrape(TEST_URL_2, "http_404");

        // then
        var gauge1 = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();
        var gauge2 = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL_2).gauge();

        assertThat(gauge1.value()).isEqualTo(1.0);
        assertThat(gauge2.value()).isEqualTo(0.0);

        var successCounter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL)
                .tag("result", "success")
                .counter();

        var failureCounter = meterRegistry.find("gtfsrt_scrape_attempts_total")
                .tag("url", TEST_URL_2)
                .tag("result", "http_404")
                .counter();

        assertThat(successCounter.count()).isEqualTo(1.0);
        assertThat(failureCounter.count()).isEqualTo(1.0);
    }
}
