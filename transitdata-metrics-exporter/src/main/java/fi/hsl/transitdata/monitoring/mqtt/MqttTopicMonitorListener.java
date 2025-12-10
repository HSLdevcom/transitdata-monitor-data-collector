package fi.hsl.transitdata.monitoring.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;

public class MqttTopicMonitorListener implements MqttCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MqttTopicMonitorListener.class);

    private static final int MQTT_KEEP_ALIVE_SECS = 20;

    private final MqttClient client;
    private final String topicName;

    private final AtomicBoolean isStarting = new AtomicBoolean(false);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final AtomicReference<Long> measuringStartedAt = new AtomicReference<>(null);
    private final AtomicReference<Long> measuringStoppedAt = new AtomicReference<>(null);

    public MqttTopicMonitorListener(MqttClient client, String topicName) {
        this.client = client;
        this.topicName = topicName;
    }

    void listenTopic() {
        if (isStarting.get()) {
            return;
        }

        isStarting.set(true);
        measuringStartedAt.set(null);
        measuringStoppedAt.set(null);
        messageCount.set(0);

        client.subscribe(topicName, this)
                .thenAccept(ignored -> {
                    isStarting.set(false);
                    isRunning.set(true);
                    measuringStartedAt.set(System.nanoTime());
                    measuringStoppedAt.set(null);
                })
                .exceptionally(ex -> {
                    isStarting.set(false);
                    isRunning.set(false);
                    measuringStoppedAt.set(System.nanoTime());
                    return null;
                });
    }

    Double messageCountSnapshot() {
        var measuringStartedAt = this.measuringStartedAt.get();
        if (measuringStartedAt == null) {
            LOG.info("No data was measured for {} on topic {}. Maybe the client was not connected?", client.getBrokerAddress(), topicName);
            return null;
        }

        var now = System.nanoTime();
        var messageCount = this.messageCount.getAndSet(0);
        var measuringStoppedAt = this.measuringStoppedAt.get();
        var elapsedTimeSeconds = Duration.ofNanos(ofNullable(measuringStoppedAt).orElse(now) - measuringStartedAt)
                .toSeconds();

        if (measuringStoppedAt != null) {
            // If data was collected for too short period, we can't accurately calculate the message rate
            if (elapsedTimeSeconds < Math.min(25, 10 * MQTT_KEEP_ALIVE_SECS)) {
                // Return null if elapsed_time is too small to calculate accurate result
                return null;
            }

            /*
              Adjust elapsed_time to account for the time it took to detect that the connection was down.
              This should take roughly 2 times the duration of MQTT keep alive interval.
              This adjustment can cause the message rate to be slightly inflated, but this is less of a problem than too small message rate, which would cause unnecessary alerts.
             */
            elapsedTimeSeconds -= 2 * MQTT_KEEP_ALIVE_SECS;
        }

        if (elapsedTimeSeconds <= 0) {
            return null;
        }

        this.measuringStartedAt.set(System.nanoTime());
        this.measuringStoppedAt.set(null);

        double messagesPerSecond = (double) messageCount / elapsedTimeSeconds;

        LOG.info("started: {}, elapsed: {}, messages: {}, rate: {}", measuringStartedAt, elapsedTimeSeconds, messageCount, messagesPerSecond);

        return messagesPerSecond;
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.warn("Disconnected from {}, cause: {}", client.getBrokerAddress(), cause == null ? "unknown" : cause.getMessage());
        measuringStoppedAt.set(System.nanoTime());
        isRunning.set(false);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        messageCount.incrementAndGet();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
