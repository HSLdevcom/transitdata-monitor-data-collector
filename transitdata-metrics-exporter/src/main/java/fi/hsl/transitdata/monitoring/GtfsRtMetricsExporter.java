package fi.hsl.transitdata.monitoring;

import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.transit.realtime.GtfsRealtime.FeedMessage;
import static java.net.http.HttpRequest.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers;
import static java.time.Instant.now;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

class GtfsRtMetricsExporter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GtfsRtMetricsExporter.class);

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(5);
    private static final int NO_DELAY = 0;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(CLIENT_TIMEOUT)
            .build();

    private final GtfsRtMetricsRegistry metricsRegistry;
    private final ScheduledExecutorService executorService;

    public GtfsRtMetricsExporter(AppConfig config, MeterRegistry registry) {
        this.metricsRegistry = new GtfsRtMetricsRegistry(registry, config.gtfsrtUrls());
        this.executorService = newScheduledThreadPool(config.gtfsrtUrls().size());

        executorService.scheduleAtFixedRate(() -> updateAllFeeds(config.gtfsrtUrls()), NO_DELAY,
                config.gtfsrtPollInterval().toMinutes(), MINUTES);
    }

    @Override
    public void close() {
        executorService.close();
    }

    private void updateAllFeeds(List<String> urls) {
        urls.forEach(this::updateFeed);
    }

    private void updateFeed(String url) {
        try {
            var req = newBuilder()
                    .GET()
                    .uri(URI.create(url))
                    .timeout(CLIENT_TIMEOUT)
                    .build();

            var resp = httpClient.send(req, BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                LOG.error("Failed to update feed for url {}, response is {}", url, resp.statusCode());
                metricsRegistry.recordFailedScrape(url, "http_" + resp.statusCode());
                return;
            }

            var feed = FeedMessage.parseFrom(resp.body());
            var entityCount = feed.getEntityCount();
            var feedTs = feed.getHeader().getTimestamp();
            var age = now().getEpochSecond() - feedTs;

            metricsRegistry.recordSuccessfulScrape(url, entityCount, (int) age);

            LOG.debug("Updated metrics for {} — entities={}, age={}s", url, entityCount, age);
        } catch (InvalidProtocolBufferException ex) {
            LOG.error("Failed to parse feed for url {}", url, ex);
            metricsRegistry.recordFailedScrape(url, "parse_error");
        } catch (IOException ex) {
            LOG.error("IO error while updating feed for url {}", url, ex);
            metricsRegistry.recordFailedScrape(url, "io_error");
        } catch (Exception ex) {
            LOG.error("Failed to update feed for url {}", url, ex);
            metricsRegistry.recordFailedScrape(url, "unknown_error");
        }
    }
}
