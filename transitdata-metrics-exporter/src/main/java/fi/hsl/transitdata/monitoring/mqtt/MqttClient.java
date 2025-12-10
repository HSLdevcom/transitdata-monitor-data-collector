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

import java.util.concurrent.CompletableFuture;

public class MqttClient {

    private static final Logger LOG = LoggerFactory.getLogger(MqttClient.class);

    private final String brokerAddress;
    private final MqttAsyncClient client;

    public MqttClient(String address, int port, String clientId) {
        this.brokerAddress = "tcp://%s:%s".formatted(address, port);

        try {
            this.client = new MqttAsyncClient(brokerAddress, clientId, new MemoryPersistence());
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Void> subscribe(String topicName, MqttCallback callback) {
        var result = new CompletableFuture<Void>();
        client.setCallback(callback);
        try {
            client.connect(defaultMqttConnectOptions(), null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    LOG.info("Connected to MQTT broker at {}", brokerAddress);

                    try {
                        client.subscribe(topicName, 0);
                        result.complete(null);
                    } catch (MqttException ex) {
                        LOG.error("Failed to subscribe to {}: {}", topicName, ex.getMessage(), ex);
                        result.completeExceptionally(ex);
                    }
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

    public String getBrokerAddress() {
        return brokerAddress;
    }

    private static MqttConnectOptions defaultMqttConnectOptions() {
        var opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setConnectionTimeout(10);
        return opts;
    }
}
