package fi.hsl.transitdata.monitoring.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class MqttClient {

    private static final Logger LOG = LoggerFactory.getLogger(MqttClient.class);

    private static final int QOS = 2;
    private static final int MAX_INFLIGHT_MESSAGES = 100;

    private final String brokerAddress;
    private final MqttAsyncClient client;
    private final MqttConnectOptions connectionOptions;

    public MqttClient(String address, int port, String clientId, Duration connectionTimeout,
            Duration keepAliveInterval) {
        this.brokerAddress = "tcp://%s:%s".formatted(address, port);
        this.connectionOptions = mqttConnectOptions(connectionTimeout, keepAliveInterval);

        try {
            this.client = new MqttAsyncClient(brokerAddress, clientId, new MemoryPersistence());
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Void> connect(MqttCallback callback) {
        var result = new CompletableFuture<Void>();
        client.setCallback(callback);

        try {
            client.connect(connectionOptions, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    LOG.info("Connected to MQTT broker at {}", brokerAddress);
                    result.complete(null);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable ex) {
                    LOG.warn("Error on connecting {}: {}", brokerAddress, ex.getMessage(), ex);
                    result.completeExceptionally(ex);
                }
            });
        } catch (MqttException ex) {
            LOG.warn("Error on connecting {}: {}", brokerAddress, ex.getMessage(), ex);
            result.completeExceptionally(ex);
        }

        return result;
    }

    public CompletableFuture<Void> subscribe(String[] topicFilters) {
        var result = new CompletableFuture<Void>();

        if (!client.isConnected()) {
            result.completeExceptionally(new IllegalStateException("Client is not connected. Call connect() first."));
            return result;
        }

        var qos = new int[topicFilters.length];
        Arrays.fill(qos, QOS);

        try {
            client.subscribe(topicFilters, qos, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    LOG.info("Subscribed to topics {} on broker {}", Arrays.toString(topicFilters), brokerAddress);
                    result.complete(null);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable ex) {
                    LOG.error("Failed to subscribe to topics {} on broker {}: {}", Arrays.toString(topicFilters),
                            brokerAddress, ex.getMessage(), ex);
                    result.completeExceptionally(ex);
                }
            });
        } catch (MqttException ex) {
            LOG.error("Failed to subscribe to topics {} on broker {}: {}", Arrays.toString(topicFilters), brokerAddress,
                    ex.getMessage(), ex);
            result.completeExceptionally(ex);
        }

        return result;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    public CompletableFuture<Void> unsubscribe(String[] topicFilters) {
        var result = new CompletableFuture<Void>();

        if (!isConnected()) {
            result.complete(null);
            return result;
        }

        try {
            client.unsubscribe(topicFilters, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    LOG.info("Unsubscribed from topics {} on broker {}", Arrays.toString(topicFilters), brokerAddress);
                    result.complete(null);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable ex) {
                    LOG.warn("Failed to unsubscribe from topics {} on broker {}: {}", Arrays.toString(topicFilters),
                            brokerAddress, ex.getMessage(), ex);
                    result.completeExceptionally(ex);
                }
            });
        } catch (MqttException ex) {
            LOG.warn("Failed to unsubscribe from topics {} on broker {}: {}", Arrays.toString(topicFilters),
                    brokerAddress, ex.getMessage(), ex);
            result.completeExceptionally(ex);
        }

        return result;
    }

    public void disconnect() {
        try {
            if (client.isConnected()) {
                client.disconnect();
                LOG.info("Disconnected from MQTT broker at {}", brokerAddress);
            }
        } catch (MqttException ex) {
            LOG.warn("Error disconnecting from {}: {}", brokerAddress, ex.getMessage(), ex);
        }
    }

    private static MqttConnectOptions mqttConnectOptions(Duration connectionTimeout, Duration keepAliveInterval) {
        var opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setConnectionTimeout((int) connectionTimeout.toSeconds());
        opts.setKeepAliveInterval((int) keepAliveInterval.toSeconds());
        opts.setMaxInflight(MAX_INFLIGHT_MESSAGES);
        return opts;
    }
}
