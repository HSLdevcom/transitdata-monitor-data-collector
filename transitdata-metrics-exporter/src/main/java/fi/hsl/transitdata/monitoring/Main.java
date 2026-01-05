package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpServer;
import fi.hsl.transitdata.monitoring.gtfsrt.GtfsRtMetricsExporter;
import fi.hsl.transitdata.monitoring.gtfsrt.GtfsRtMetricsRegistry;
import fi.hsl.transitdata.monitoring.mqtt.MqttClient;
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

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
        mqttListeners.start();

        var closeables = new ArrayList<Closeable>();
        closeables.add(healthEndpoint);
        closeables.add(gtfsRtMetricsExporter);
        closeables.add(mqttListeners);

        healthEndpoint.markReady();

        getRuntime().addShutdownHook(new Thread(() -> shutdown(closeables)));

        LOG.info("Application started on port {}", config.port());
    }

    private static GtfsRtMetricsExporter createGtfsRtMetricsExporter(AppConfig config,
            PrometheusMeterRegistry registry) {
        var httpClient = HttpClient.newBuilder().connectTimeout(config.gtfsRtClientTimeout()).build();
        var gtfsRtMetricsRegistry = new GtfsRtMetricsRegistry(registry, config.gtfsRtUrls());
        var executor = newScheduledThreadPool(config.gtfsRtUrls().size());

        return new GtfsRtMetricsExporter(config, httpClient, gtfsRtMetricsRegistry, executor);
    }

    private static MqttListeners createMqttListeners(AppConfig config, PrometheusMeterRegistry registry) {
        var listeners = config.mqttBrokers().stream().map(brokerConfig -> {
            var client = new MqttClient(brokerConfig.address(), brokerConfig.port(), MqttClientId.get(),
                    config.mqttConnectionTimeout(), config.mqttKeepAliveInterval());
            return new MqttTopicMonitorListener(client, brokerConfig.topicFilters(), registry);
        }).toList();

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
