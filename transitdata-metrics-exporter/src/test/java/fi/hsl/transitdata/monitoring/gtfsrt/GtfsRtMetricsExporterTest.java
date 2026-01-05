package fi.hsl.transitdata.monitoring.gtfsrt;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import fi.hsl.transitdata.monitoring.AppConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.transit.realtime.GtfsRealtime.FeedHeader;
import static com.google.transit.realtime.GtfsRealtime.FeedMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GtfsRtMetricsExporterTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse<byte[]> httpResponse;

    @Mock
    private ScheduledExecutorService scheduledExecutor;

    private SimpleMeterRegistry meterRegistry;
    private AppConfig config;
    private GtfsRtMetricsRegistry metricsRegistry;
    private GtfsRtMetricsExporter exporter;

    private static final String TEST_URL = "http://example.com/gtfs-rt";
    private static final Duration POLL_INTERVAL = Duration.ofMinutes(1);
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(5);

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        config = new AppConfig(8080, List.of(TEST_URL), POLL_INTERVAL, CLIENT_TIMEOUT, CLIENT_TIMEOUT, CLIENT_TIMEOUT, List.of());
        metricsRegistry = new GtfsRtMetricsRegistry(meterRegistry, config.gtfsRtUrls());
    }

    @Test
    void shouldSchedulePeriodicUpdateOnConstruction() {
        // given / when
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        // then
        assertThat(exporter).isNotNull();
    }

    @Test
    void shouldCloseExecutorServiceWhenClosed() {
        // given
        var realExecutor = Executors.newScheduledThreadPool(1);
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, realExecutor);

        // when
        exporter.close();

        // then
        assertThat(realExecutor.isShutdown()).isTrue();
    }

    @Test
    void shouldRecordSuccessfulScrapeWhenHttpStatusIs200() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        var feedMessage = createValidFeedMessage(100, 1234567890L);
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(feedMessage.toByteArray());
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var successCounter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "success").counter();

        assertThat(successCounter).isNotNull();
        assertThat(successCounter.count()).isEqualTo(1.0);
    }

    @Test
    void shouldRecordFailedScrapeWhenHttpStatusIsNot200() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        when(httpResponse.statusCode()).thenReturn(404);
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var failureCounter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "http_404").counter();

        var lastScrapeSuccess = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(failureCounter).isNotNull();
        assertThat(failureCounter.count()).isEqualTo(1.0);
        assertThat(lastScrapeSuccess.value()).isEqualTo(0.0);
    }

    @Test
    void shouldRecordParseErrorWhenInvalidProtobuf() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("invalid protobuf data".getBytes());
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var parseErrorCounter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "parse_error").counter();

        var lastScrapeSuccess = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(parseErrorCounter).isNotNull();
        assertThat(parseErrorCounter.count()).isEqualTo(1.0);
        assertThat(lastScrapeSuccess.value()).isEqualTo(0.0);
    }

    @Test
    void shouldRecordIoErrorWhenHttpClientThrowsIOException() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenThrow(new IOException("Network error"));

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var ioErrorCounter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "io_error").counter();

        var lastScrapeSuccess = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(ioErrorCounter).isNotNull();
        assertThat(ioErrorCounter.count()).isEqualTo(1.0);
        assertThat(lastScrapeSuccess.value()).isEqualTo(0.0);
    }

    @Test
    void shouldRecordUnknownErrorWhenUnexpectedExceptionOccurs() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenThrow(new RuntimeException("Unexpected error"));

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var unknownErrorCounter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "unknown_error").counter();

        var lastScrapeSuccess = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(unknownErrorCounter).isNotNull();
        assertThat(unknownErrorCounter.count()).isEqualTo(1.0);
        assertThat(lastScrapeSuccess.value()).isEqualTo(0.0);
    }

    @Test
    void shouldSetLastScrapeSuccessToOneWhenScrapeSucceeds() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        var feedMessage = createValidFeedMessage(50, 9876543210L);
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(feedMessage.toByteArray());
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var lastScrapeSuccess = meterRegistry.find("gtfsrt_last_scrape_success").tag("url", TEST_URL).gauge();

        assertThat(lastScrapeSuccess).isNotNull();
        assertThat(lastScrapeSuccess.value()).isEqualTo(1.0);
    }

    @Test
    void shouldRecordEntityCountAndTimestampAgeWhenScrapeSucceeds() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        var entityCount = 42;
        var timestamp = System.currentTimeMillis() / 1000 - 10; // 10 seconds ago
        var feedMessage = createValidFeedMessage(entityCount, timestamp);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(feedMessage.toByteArray());
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(TEST_URL);

        // then
        var entityCountSummary = meterRegistry.find("gtfsrt_entity_count").tag("url", TEST_URL).summary();

        var timestampAgeSummary = meterRegistry.find("gtfsrt_timestamp_age_seconds").tag("url", TEST_URL).summary();

        assertThat(entityCountSummary).isNotNull();
        assertThat(entityCountSummary.count()).isEqualTo(1);
        assertThat(entityCountSummary.totalAmount()).isEqualTo(entityCount);

        assertThat(timestampAgeSummary).isNotNull();
        assertThat(timestampAgeSummary.count()).isEqualTo(1);

        assertThat(timestampAgeSummary.totalAmount()).isCloseTo(10d, offset(1d));
    }

    @Test
    void shouldHandleMultipleUrlsIndependently() throws Exception {
        // given
        var url1 = "http://example.com/feed1";
        var url2 = "http://example.com/feed2";
        var multiUrlConfig = new AppConfig(8080, List.of(url1, url2), POLL_INTERVAL, CLIENT_TIMEOUT, CLIENT_TIMEOUT, CLIENT_TIMEOUT, List.of());
        var multiUrlRegistry = new GtfsRtMetricsRegistry(meterRegistry, multiUrlConfig.gtfsRtUrls());
        exporter = new GtfsRtMetricsExporter(multiUrlConfig, httpClient, multiUrlRegistry, scheduledExecutor);

        var feedMessage1 = createValidFeedMessage(100, System.currentTimeMillis() / 1000);
        var feedMessage2 = createValidFeedMessage(200, System.currentTimeMillis() / 1000);

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(feedMessage1.toByteArray()).thenReturn(feedMessage2.toByteArray());
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);

        // when
        exporter.updateFeed(url1);
        exporter.updateFeed(url2);

        // then
        var summary1 = meterRegistry.find("gtfsrt_entity_count").tag("url", url1).summary();
        var summary2 = meterRegistry.find("gtfsrt_entity_count").tag("url", url2).summary();

        assertThat(summary1.totalAmount()).isEqualTo(100);
        assertThat(summary2.totalAmount()).isEqualTo(200);
    }

    @Test
    void shouldTrackDifferentHttpErrorCodesSeparately() throws Exception {
        // given
        exporter = new GtfsRtMetricsExporter(config, httpClient, metricsRegistry, scheduledExecutor);

        // when
        when(httpResponse.statusCode()).thenReturn(404);
        when(httpClient.send(any(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofByteArray())))
                .thenReturn(httpResponse);
        exporter.updateFeed(TEST_URL);

        when(httpResponse.statusCode()).thenReturn(500);
        exporter.updateFeed(TEST_URL);

        when(httpResponse.statusCode()).thenReturn(503);
        exporter.updateFeed(TEST_URL);

        // then
        var http404Counter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "http_404").counter();

        var http500Counter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "http_500").counter();

        var http503Counter = meterRegistry.find("gtfsrt_scrape_attempts_total").tag("url", TEST_URL)
                .tag("result", "http_503").counter();

        assertThat(http404Counter.count()).isEqualTo(1.0);
        assertThat(http500Counter.count()).isEqualTo(1.0);
        assertThat(http503Counter.count()).isEqualTo(1.0);
    }

    private FeedMessage createValidFeedMessage(int entityCount, long timestamp) {
        var header = FeedHeader.newBuilder().setGtfsRealtimeVersion("2.0").setTimestamp(timestamp).build();

        var builder = FeedMessage.newBuilder().setHeader(header);

        for (int i = 0; i < entityCount; i++) {
            builder.addEntity(FeedEntity.newBuilder().setId("entity_" + i).build());
        }

        return builder.build();
    }
}
