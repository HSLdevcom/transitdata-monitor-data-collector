package fi.hsl.transitdata.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

class GtfsRtMetricsRegistry {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, DistributionSummary> entityCount = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DistributionSummary> timestampAge = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> lastScrapeSuccess = new ConcurrentHashMap<>();

    public GtfsRtMetricsRegistry(MeterRegistry registry, List<String> urls) {
        this.registry = registry;
        registerMetrics(urls);
    }

    private void registerMetrics(List<String> urls) {
        urls.forEach(url -> {
            entityCount.put(url, DistributionSummary.builder("gtfsrt_entity_count")
                    .description("Number of GTFS-RT entities in the feed")
                    .tag("url", url)
                    .register(registry));

            timestampAge.put(url, DistributionSummary.builder("gtfsrt_timestamp_age_seconds")
                    .description("Age in seconds of the GTFS-RT feed header timestamp")
                    .baseUnit("seconds")
                    .tag("url", url)
                    .register(registry));

            lastScrapeSuccess.put(url, new AtomicInteger(0));
            Gauge.builder("gtfsrt_last_scrape_success", lastScrapeSuccess.get(url), AtomicInteger::get)
                    .description("Whether the last scrape was successful (1) or not (0)").tag("url", url)
                    .register(registry);
        });
    }

    public void recordSuccessfulScrape(String url, int entityCount, int timestampAgeSeconds) {
        this.entityCount.get(url).record(entityCount);
        this.timestampAge.get(url).record(timestampAgeSeconds);
        this.lastScrapeSuccess.get(url).set(1);
        incrementScrapeCounter(url, "success");
    }

    public void recordFailedScrape(String url, String result) {
        this.lastScrapeSuccess.get(url).set(0);
        incrementScrapeCounter(url, result);
    }

    private void incrementScrapeCounter(String url, String result) {
        Counter.builder("gtfsrt_scrape_attempts_total")
                .tag("url", url)
                .tag("result", result)
                .register(registry)
                .increment();
    }
}
