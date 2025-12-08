package fi.hsl.transitdata.monitoring;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.transit.realtime.GtfsRealtime.FeedMessage;
import static java.net.http.HttpRequest.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers;
import static java.time.Instant.now;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class GtfsRtMetricsExporter {

    private static final Logger LOG = LoggerFactory.getLogger(GtfsRtMetricsExporter.class);

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(5);
    private static final int NO_DELAY = 0;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(CLIENT_TIMEOUT)
            .build();

    private final MeterRegistry registry;
    private final ScheduledExecutorService executorService = newScheduledThreadPool(1);
    private final ConcurrentMap<String, AtomicInteger> entityCountMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> ageMap = new ConcurrentHashMap<>();

    public GtfsRtMetricsExporter(AppConfig config, MeterRegistry registry) {
        this.registry = registry;
        registerMetrics(config.gtfsrtUrls());

        executorService.scheduleAtFixedRate(
                () -> updateAllFeeds(config.gtfsrtUrls()),
                NO_DELAY,
                config.gtfsrtPollInterval().toMinutes(),
                MINUTES
        );
    }

    public void close() {
        executorService.close();
    }

    private void registerMetrics(List<String> urls) {
        urls.forEach(url -> {
            entityCountMap.put(url, new AtomicInteger(0));
            ageMap.put(url, new AtomicInteger(0));

            Gauge.builder("gtfsrt_entity_count", entityCountMap.get(url), AtomicInteger::get)
                    .description("Number of GTFS-RT entities in the feed")
                    .tag("url", url)
                    .register(registry);

            Gauge.builder("gtfsrt_timestamp_age_seconds", ageMap.get(url), AtomicInteger::get)
                    .description("Age in seconds of the GTFS-RT feed header timestamp")
                    .tag("url", url)
                    .register(registry);
        });
    }

    private void updateAllFeeds(List<String> urls) {
        urls.forEach(this::updateFeed);
    }

    private void updateFeed(String url) {
        try {
            var req = newBuilder().GET()
                    .uri(URI.create(url))
                    .timeout(CLIENT_TIMEOUT)
                    .build();

            var resp = httpClient.send(req, BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                LOG.error("Failed to update feed for url {}, response is {}", url, resp.statusCode());
                return;
            }

            var feed = FeedMessage.parseFrom(resp.body());
            var entityCount = feed.getEntityCount();
            var feedTs = feed.getHeader().getTimestamp();
            var age = now().getEpochSecond() - feedTs;

            entityCountMap.get(url).set(entityCount);
            ageMap.get(url).set((int) age);

            LOG.debug("Updated metrics for {} — entities={}, age={}s", url, entityCount, age);
        } catch (Exception ex) {
            LOG.error("Failed to update feed for url {}", url, ex);
        }
    }
}
