package fi.hsl.transitdata.monitoring.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static fi.hsl.transitdata.monitoring.mqtt.MqttTopicFilterMatcher.findMatchingTopicFilter;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MqttTopicMonitorListener implements MqttCallback, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTopicMonitorListener.class);

    private final MqttClient client;
    private final String[] topicFilters;
    private final MeterRegistry registry;
    private final ExecutorService resubscriptionExecutor;

    public MqttTopicMonitorListener(MqttClient client, List<String> topicFilters, MeterRegistry registry) {
        this.client = client;
        this.topicFilters = topicFilters.toArray(new String[0]);
        this.registry = registry;
        this.resubscriptionExecutor = newSingleThreadExecutor();

        Gauge.builder("mqtt_connected", client, c -> c.isConnected() ? 1.0 : 0.0)
                .description("MQTT connection status (1 = connected, 0 = disconnected)")
                .tag("broker", client.getBrokerAddress())
                .register(registry);
    }

    public CompletableFuture<Void> start() {
        return client.connect(this)
                .thenCompose(ignored -> client.subscribe(topicFilters))
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        LOG.error("Failed to connect or subscribe to {}: {}", client.getBrokerAddress(),
                                ex.getMessage(), ex);
                    } else {
                        LOG.info("Successfully connected and subscribed to topics on {}", client.getBrokerAddress());
                    }
                });
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.warn("Connection lost from {}: {}", client.getBrokerAddress(),
                cause == null ? "unknown" : cause.getMessage(), cause);
        scheduleResubscription();
    }

    private void scheduleResubscription() {
        delayedExecutor(60, SECONDS, resubscriptionExecutor).execute(() -> {
            if (client.isConnected()) {
                LOG.info("Reconnected to {}, attempting to re-subscribe", client.getBrokerAddress());

                client.subscribe(topicFilters)
                        .thenAccept(ignored -> LOG.info("Successfully re-subscribed to topics on {}",
                                client.getBrokerAddress()))
                        .exceptionally(ex -> {
                            LOG.warn("Re-subscription failed for {}: {}", client.getBrokerAddress(), ex.getMessage());
                            scheduleResubscription();
                            return null;
                        });
            } else {
                LOG.info("Not yet reconnected to {}, will retry re-subscription", client.getBrokerAddress());
                scheduleResubscription();
            }
        });
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        Counter.builder("mqtt_messages_received_total")
                .description("Total MQTT messages received")
                .tag("broker", client.getBrokerAddress())
                .tag("topic_filter", findMatchingTopicFilter(topic, topicFilters).orElse("unknown"))
                .tag("qos", String.valueOf(message.getQos()))
                .tag("is_duplicate", String.valueOf(message.isDuplicate()))
                .tag("is_retained", String.valueOf(message.isRetained()))
                .register(registry)
                .increment();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void close() {
        try {
            resubscriptionExecutor.shutdown();

            client.unsubscribe(topicFilters).thenRunAsync(client::disconnect).exceptionallyAsync(ex -> {
                LOG.warn("Error during unsubscribe, disconnecting anyway: {}", ex.getMessage());
                runAsync(client::disconnect).join();
                return null;
            }).join();

            if (!resubscriptionExecutor.awaitTermination(5, SECONDS)) {
                LOG.warn("Re-subscription executor did not terminate, forcing shutdown");
                resubscriptionExecutor.shutdownNow();
            }
        } catch (Exception ex) {
            LOG.warn("Error during close, forcing disconnect: {}", ex.getMessage());
            runAsync(client::disconnect).join();
            resubscriptionExecutor.shutdownNow();
        }
    }
}
