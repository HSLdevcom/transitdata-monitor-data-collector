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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fi.hsl.transitdata.monitoring.mqtt.MqttTopicFilterMatcher.findMatchingTopicFilters;

public class MqttTopicMonitorListener implements MqttCallbackExtended, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTopicMonitorListener.class);

    private static final int QOS = 2;
    /**
     * The protocol maximum is 2**16 - 1 for QoS 1 and 2 (-1 for the forbidden Packet Identifier 0).
     * We get from the HFP topic alone around 110_000 messages per minute at peak time.
     * That would be about 1833 messages per second just for that topic filter.
     * If the worst case RTT between the data centers would be 100 ms, that would mean a throughput
     * of MAX_INFLIGHT_MESSAGES / 0.1 s. With max of 100 msg, the throughput would be only 1000 msg/s.
     * With max of 1000 msg, the throughput would be 10000 msg/s which is sufficient.
     */
    private static final int MAX_INFLIGHT_MESSAGES = 1000;

    private final MqttAsyncClient client;
    private final MqttConnectOptions connectOptions;
    private final String brokerAddress;
    private final String[] topicFilters;
    private final Map<String, Counter> messageCounters;
    private final Counter connectionLostCounter;

    public MqttTopicMonitorListener(String brokerAddress, String clientId, List<String> topicFilters,
            Duration connectionTimeout, Duration keepAliveInterval, MeterRegistry registry) {
        this.brokerAddress = brokerAddress;
        this.topicFilters = topicFilters.toArray(new String[0]);
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

        this.messageCounters = registerMessageCounters(brokerAddress, topicFilters, registry);
        this.connectionLostCounter = Counter.builder("mqtt_connection_lost")
                .description("MQTT connection lost")
                .tag("broker", brokerAddress)
                .register(registry);
    }

    private static Map<String, Counter> registerMessageCounters(String brokerAddress, List<String> topicFilters,
            MeterRegistry registry) {
        var counters = new HashMap<String, Counter>();
        for (var topicFilter : topicFilters) {
            counters.put(topicFilter,
                    Counter.builder("mqtt_messages_received_total")
                            .description("Total MQTT messages received")
                            .tag("broker", brokerAddress)
                            .tag("topic_filter", topicFilter)
                            .register(registry));
        }
        counters.put("unknown",
                Counter.builder("mqtt_messages_received_total")
                        .description("Total MQTT messages received")
                        .tag("broker", brokerAddress)
                        .tag("topic_filter", "unknown")
                        .register(registry));

        return Map.copyOf(counters);
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
        connectionLostCounter.increment();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        var matchingFilters = findMatchingTopicFilters(topic, topicFilters);
        if (matchingFilters.isEmpty()) {
            messageCounters.get("unknown").increment();
            return;
        }

        for (var topicFilter : matchingFilters) {
            messageCounters.get(topicFilter).increment();
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void close() {
        try {
            if (client.isConnected()) {
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
        opts.setCleanSession(false);
        opts.setConnectionTimeout((int) connectionTimeout.toSeconds());
        opts.setKeepAliveInterval((int) keepAliveInterval.toSeconds());
        opts.setMaxInflight(MAX_INFLIGHT_MESSAGES);
        return opts;
    }
}
