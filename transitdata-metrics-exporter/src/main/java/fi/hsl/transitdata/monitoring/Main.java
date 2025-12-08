package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.lang.Runtime.getRuntime;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        var config = AppConfig.parseFrom("application.conf");
        var httpServer = HttpServer.create(new InetSocketAddress(config.port()), 0);
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        httpServer.createContext("/metrics", exchange -> {
            var response = registry.scrape();
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        var gtfsRtMetricsExporter = new GtfsRtMetricsExporter(config, registry);

        httpServer.start();

        getRuntime().addShutdownHook(new Thread(() -> close(gtfsRtMetricsExporter)));

        LOG.info("Application started on port {}", config.port());
    }

    private static void close(GtfsRtMetricsExporter gtfsRtMetricsExporter) {
        try {
            gtfsRtMetricsExporter.close();

            LOG.info("Application closed");
        } catch (Exception ex) {
            LOG.warn("Got exception handling application close", ex);
        }
    }
}
