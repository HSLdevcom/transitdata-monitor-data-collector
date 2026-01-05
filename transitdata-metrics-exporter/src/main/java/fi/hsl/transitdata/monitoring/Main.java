package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpServer;
import fi.hsl.transitdata.monitoring.mqtt.MqttClient;
import fi.hsl.transitdata.monitoring.mqtt.MqttClientId;
import fi.hsl.transitdata.monitoring.mqtt.MqttTopicMonitorListener;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        var config = AppConfig.parseFrom("application.conf");
        var httpServer = HttpServer.create(new InetSocketAddress(config.port()), 0);
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        var healthEndpoint = new HealthEndpoint();
        httpServer.createContext("/liveness", new LivenessEndpoint());
        httpServer.createContext("/health", healthEndpoint);
        httpServer.createContext("/metrics", new MetricsEndpoint(registry));

        var gtfsRtMetricsExporter = createGtfsRtMetricsExporter(config, registry);
        var mqttListeners = createMqttListeners(config, registry);

        httpServer.start();
        gtfsRtMetricsExporter.start();

        var mqttFutures = mqttListeners.stream()
                .map(MqttTopicMonitorListener::start)
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(mqttFutures).join();

        var closeables = new ArrayList<Closeable>();
        closeables.add(gtfsRtMetricsExporter);
        closeables.addAll(mqttListeners);

        healthEndpoint.markReady();

        getRuntime().addShutdownHook(new Thread(() -> shutdown(healthEndpoint, closeables)));

        LOG.info("Application started on port {}", config.port());
    }

    private static GtfsRtMetricsExporter createGtfsRtMetricsExporter(AppConfig config,
                                                                     PrometheusMeterRegistry registry) {
        var httpClient = HttpClient.newBuilder().connectTimeout(config.gtfsRtClientTimeout()).build();
        var gtfsRtMetricsRegistry = new GtfsRtMetricsRegistry(registry, config.gtfsRtUrls());
        var executor = newScheduledThreadPool(config.gtfsRtUrls().size());

        return new GtfsRtMetricsExporter(config, httpClient, gtfsRtMetricsRegistry, executor);
    }

    private static List<MqttTopicMonitorListener> createMqttListeners(AppConfig config, PrometheusMeterRegistry registry) {
        return config.mqttBrokers().stream()
                .map(brokerConfig -> {
                    var client = new MqttClient(
                            brokerConfig.address(),
                            brokerConfig.port(),
                            MqttClientId.get(),
                            config.mqttConnectionTimeout(),
                            config.mqttKeepAliveInterval()
                    );
                    var topicFilters = brokerConfig.topicFilters().toArray(new String[0]);
                    return new MqttTopicMonitorListener(client, topicFilters, registry);
                })
                .toList();
    }

    private static void shutdown(HealthEndpoint healthEndpoint, List<Closeable> closeables) {
        try {
            LOG.info("Shutting down application...");
            healthEndpoint.markNotReady();

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
