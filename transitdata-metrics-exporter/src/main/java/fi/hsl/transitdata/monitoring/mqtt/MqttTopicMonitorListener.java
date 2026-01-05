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
import java.util.concurrent.CompletableFuture;

import static fi.hsl.transitdata.monitoring.mqtt.MqttTopicFilterMatcher.findMatchingTopicFilter;

public class MqttTopicMonitorListener implements MqttCallback, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTopicMonitorListener.class);

    private final MqttClient client;
    private final String[] topicFilters;
    private final MeterRegistry meterRegistry;

    public MqttTopicMonitorListener(MqttClient client, String[] topicFilters, MeterRegistry meterRegistry) {
        this.client = client;
        this.topicFilters = topicFilters;
        this.meterRegistry = meterRegistry;

        Gauge.builder("mqtt_connected", client, c -> c.isConnected() ? 1.0 : 0.0)
                .description("MQTT connection status (1 = connected, 0 = disconnected)")
                .tag("broker", client.getBrokerAddress())
                .register(meterRegistry);
    }

    public CompletableFuture<Void> start() {
        return client.connect(this)
                .thenCompose(ignored -> client.subscribe(topicFilters))
                .thenAccept(ignored -> LOG.info("Successfully connected and subscribed to topics on {}", client.getBrokerAddress()))
                .exceptionally(ex -> {
                    LOG.error("Failed to connect or subscribe to {}: {}", client.getBrokerAddress(), ex.getMessage(), ex);
                    return null;
                });
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.warn("Disconnected from {}: {}",
                client.getBrokerAddress(), cause == null ? "unknown" : cause.getMessage(), cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        Counter.builder("mqtt_messages_received_total")
                .description("Total MQTT messages received")
                .tag("broker", client.getBrokerAddress())
                .tag("topic", topic)
                .tag("topic_filter", findMatchingTopicFilter(topic, topicFilters).orElse("unknown"))
                .tag("qos", String.valueOf(message.getQos()))
                .tag("is_duplicate", String.valueOf(message.isDuplicate()))
                .tag("is_retained", String.valueOf(message.isRetained()))
                .register(meterRegistry)
                .increment();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void close() {
        try {
            client.unsubscribe(topicFilters)
                    .thenRun(client::disconnect)
                    .exceptionally(ex -> {
                        LOG.warn("Error during unsubscribe, disconnecting anyway: {}", ex.getMessage());
                        client.disconnect();
                        return null;
                    })
                    .join();
        } catch (Exception ex) {
            LOG.warn("Error during close, forcing disconnect: {}", ex.getMessage());
            client.disconnect();
        }
    }
}
