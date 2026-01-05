package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        var config = AppConfig.parseFrom("application.conf");
        var httpServer = HttpServer.create(new InetSocketAddress(config.port()), 0);
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        httpServer.createContext("/metrics", new MetricsEndpoint(registry));
        httpServer.createContext("/health", new HealthEndpoint());

        var gtfsRtMetricsExporter = createGtfsRtMetricsExporter(config, registry);

        httpServer.start();

        getRuntime().addShutdownHook(new Thread(() -> close(gtfsRtMetricsExporter)));

        LOG.info("Application started on port {}", config.port());
    }

    private static GtfsRtMetricsExporter createGtfsRtMetricsExporter(AppConfig config,
            PrometheusMeterRegistry registry) {
        var httpClient = HttpClient.newBuilder().connectTimeout(config.gtfsRtClientTimeout()).build();
        var gtfsRtMetricsRegistry = new GtfsRtMetricsRegistry(registry, config.gtfsRtUrls());
        var executor = newScheduledThreadPool(config.gtfsRtUrls().size());

        return new GtfsRtMetricsExporter(config, httpClient, gtfsRtMetricsRegistry, executor);
    }

    private static void close(Closeable... closeables) {
        try {
            if (closeables != null) {
                for (var closeable : closeables) {
                    try {
                        closeable.close();
                    } catch (IOException ex) {
                        LOG.warn("Failed to close {}", closeable.getClass().getSimpleName(), ex);
                    }
                }
            }

            LOG.info("Application closed");
        } catch (Exception ex) {
            LOG.warn("Got exception handling application close", ex);
        }
    }
}
