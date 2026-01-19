package fi.hsl.transitdata.monitoring.mqtt;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static fi.hsl.transitdata.monitoring.mqtt.MqttTopicFilterMatcher.findMatchingTopicFilter;

public class MqttTopicMonitorListener implements MqttCallbackExtended, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTopicMonitorListener.class);

    private static final int QOS = 2;
    private static final int MAX_INFLIGHT_MESSAGES = 1000;

    private final MqttAsyncClient client;
    private final MqttConnectOptions connectOptions;
    private final String brokerAddress;
    private final String[] topicFilters;
    private final MeterRegistry registry;

    public MqttTopicMonitorListener(String brokerAddress, String clientId, List<String> topicFilters,
            Duration connectionTimeout, Duration keepAliveInterval, MeterRegistry registry) {
        this.brokerAddress = brokerAddress;
        this.topicFilters = topicFilters.toArray(new String[0]);
        this.registry = registry;
        this.connectOptions = createConnectOptions(connectionTimeout, keepAliveInterval);

        try {
            this.client = new MqttAsyncClient(brokerAddress, clientId, new MemoryPersistence());
            this.client.setCallback(this);
        } catch (MqttException e) {
            throw new RuntimeException("Failed to create MQTT client for " + brokerAddress, e);
        }

        Gauge.builder("mqtt_connected", client, c -> c.isConnected() ? 1.0 : 0.0)
                .description("MQTT connection status (1 = connected, 0 = disconnected)")
                .tag("broker", brokerAddress)
                .register(registry);
    }

    public void start() {
        try {
            LOG.info("Connecting to MQTT broker {}", brokerAddress);
            client.connect(connectOptions);
        } catch (MqttException e) {
            LOG.error("Failed to initiate connection to {}: {}", brokerAddress, e.getMessage(), e);
        }
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        LOG.info("{} to {}, subscribing to topics", reconnect ? "Reconnected" : "Connected", serverURI);

        var qos = new int[topicFilters.length];
        Arrays.fill(qos, QOS);

        try {
            client.subscribe(topicFilters, qos);
            LOG.info("Subscribed to {} topic filter(s) on {}", topicFilters.length, serverURI);
        } catch (MqttException e) {
            LOG.error("Subscription failed for {}: {}", serverURI, e.getMessage(), e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.warn("Connection lost from {}: {}", brokerAddress, cause == null ? "unknown" : cause.getMessage(), cause);

        Counter.builder("mqtt_connection_lost")
                .description("MQTT connection lost")
                .tag("broker", brokerAddress)
                .register(registry)
                .increment();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        Counter.builder("mqtt_messages_received_total")
                .description("Total MQTT messages received")
                .tag("broker", brokerAddress)
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
            if (client.isConnected()) {
                client.unsubscribe(topicFilters);
                client.disconnect();
            }
        } catch (MqttException e) {
            LOG.warn("Error during close for {}: {}", brokerAddress, e.getMessage());
            try {
                client.disconnectForcibly();
            } catch (MqttException ex) {
                LOG.warn("Forced disconnect failed for {}: {}", brokerAddress, ex.getMessage());
            }
        }
    }

    private static MqttConnectOptions createConnectOptions(Duration connectionTimeout, Duration keepAliveInterval) {
        var opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setConnectionTimeout((int) connectionTimeout.toSeconds());
        opts.setKeepAliveInterval((int) keepAliveInterval.toSeconds());
        opts.setMaxInflight(MAX_INFLIGHT_MESSAGES);
        return opts;
    }
}
