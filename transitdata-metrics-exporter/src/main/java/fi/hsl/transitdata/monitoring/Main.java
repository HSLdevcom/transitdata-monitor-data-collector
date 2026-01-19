package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpServer;
import fi.hsl.transitdata.monitoring.gtfsrt.GtfsRtMetricsExporter;
import fi.hsl.transitdata.monitoring.gtfsrt.GtfsRtMetricsRegistry;
import fi.hsl.transitdata.monitoring.mqtt.MqttClientId;
import fi.hsl.transitdata.monitoring.mqtt.MqttListeners;
import fi.hsl.transitdata.monitoring.mqtt.MqttTopicMonitorListener;
import fi.hsl.transitdata.monitoring.web.HealthEndpoint;
import fi.hsl.transitdata.monitoring.web.LivenessEndpoint;
import fi.hsl.transitdata.monitoring.web.MetricsEndpoint;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        var closeables = new ArrayList<Closeable>();

        try {
            var config = AppConfig.parseFrom("application.conf");
            var httpServer = HttpServer.create(new InetSocketAddress(config.port()), 0);
            var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

            var healthEndpoint = new HealthEndpoint();
            httpServer.createContext("/liveness", new LivenessEndpoint());
            httpServer.createContext("/health", healthEndpoint);
            httpServer.createContext("/metrics", new MetricsEndpoint(registry));

            var gtfsRtMetricsExporter = createGtfsRtMetricsExporter(config, registry);
            var mqttListeners = createMqttListeners(config, registry);

            closeables.add(healthEndpoint);
            closeables.add(gtfsRtMetricsExporter);
            closeables.add(mqttListeners);

            httpServer.start();
            gtfsRtMetricsExporter.start();
            mqttListeners.start();

            healthEndpoint.markReady();

            getRuntime().addShutdownHook(new Thread(() -> shutdown(closeables)));

            LOG.info("Application started on port {}", config.port());
        } catch (Exception ex) {
            LOG.error("Application startup failed", ex);
            shutdown(closeables);
            System.exit(1);
        }
    }

    private static GtfsRtMetricsExporter createGtfsRtMetricsExporter(AppConfig config,
                                                                     PrometheusMeterRegistry registry) {
        var httpClient = HttpClient.newBuilder().connectTimeout(config.gtfsRtClientTimeout()).build();
        var gtfsRtMetricsRegistry = new GtfsRtMetricsRegistry(registry, config.gtfsRtUrls());
        var executor = newScheduledThreadPool(config.gtfsRtUrls().size(), Thread.ofVirtual().factory());

        return new GtfsRtMetricsExporter(config, httpClient, gtfsRtMetricsRegistry, executor);
    }

    private static MqttListeners createMqttListeners(AppConfig config, PrometheusMeterRegistry registry) {
        var listeners = config.mqttBrokers().stream().map(broker ->
                new MqttTopicMonitorListener(
                        broker.address(),
                        MqttClientId.get(),
                        broker.topicFilters(),
                        config.mqttConnectionTimeout(),
                        config.mqttKeepAliveInterval(),
                        registry)
        ).toList();

        return new MqttListeners(listeners);
    }

    private static void shutdown(List<Closeable> closeables) {
        try {
            LOG.info("Shutting down application...");

            for (var closeable : closeables) {
                try {
                    closeable.close();
                } catch (Exception ex) {
                    LOG.warn("Failed to close {}", closeable.getClass().getSimpleName(), ex);
                }
            }

            LOG.info("Application shutdown");
        } catch (Exception ex) {
            LOG.warn("Error during shutdown", ex);
        }
    }
}
