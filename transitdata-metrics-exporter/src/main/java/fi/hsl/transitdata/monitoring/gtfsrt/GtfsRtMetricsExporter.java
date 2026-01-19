package fi.hsl.transitdata.monitoring.gtfsrt;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.transitdata.monitoring.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.transit.realtime.GtfsRealtime.FeedMessage;
import static java.net.http.HttpRequest.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers;
import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GtfsRtMetricsExporter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GtfsRtMetricsExporter.class);

    private final AppConfig config;
    private final HttpClient httpClient;
    private final GtfsRtMetricsRegistry registry;
    private final ScheduledExecutorService executor;

    public GtfsRtMetricsExporter(AppConfig config, HttpClient httpClient, GtfsRtMetricsRegistry registry,
            ScheduledExecutorService executor) {
        this.config = config;
        this.httpClient = httpClient;
        this.registry = registry;
        this.executor = executor;
    }

    public void start() {
        config.gtfsRtUrls()
                .forEach(url -> executor.scheduleAtFixedRate(() -> updateFeed(url), 0,
                        config.gtfsRtPollInterval().toSeconds(), SECONDS));
    }

    @Override
    public void close() {
        executor.close();
    }

    void updateFeed(String url) {
        try {
            var req = newBuilder().GET().uri(URI.create(url)).timeout(config.gtfsRtClientTimeout()).build();

            var resp = httpClient.send(req, BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                LOG.error("Failed to update feed for url {}, response is {}", url, resp.statusCode());
                registry.recordFailedScrape(url, "http_" + resp.statusCode());
                return;
            }

            var feed = FeedMessage.parseFrom(resp.body());
            var entityCount = feed.getEntityCount();
            var feedTs = feed.getHeader().getTimestamp();
            var age = now().getEpochSecond() - feedTs;

            registry.recordSuccessfulScrape(url, entityCount, (int) age);

            LOG.debug("Updated metrics for {} — entities={}, age={}s", url, entityCount, age);
        } catch (InvalidProtocolBufferException ex) {
            LOG.error("Failed to parse feed for url {}", url, ex);
            registry.recordFailedScrape(url, "parse_error");
        } catch (IOException ex) {
            LOG.error("IO error while updating feed for url {}", url, ex);
            registry.recordFailedScrape(url, "io_error");
        } catch (Exception ex) {
            LOG.error("Failed to update feed for url {}", url, ex);
            registry.recordFailedScrape(url, "unknown_error");
        }
    }
}
