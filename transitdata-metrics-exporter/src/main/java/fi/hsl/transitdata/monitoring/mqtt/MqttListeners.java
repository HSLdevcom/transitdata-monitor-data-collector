package fi.hsl.transitdata.monitoring.mqtt;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public record MqttListeners(List<MqttTopicMonitorListener> listeners) implements Closeable {

    public void start() {
        var mqttFutures = listeners.stream().map(MqttTopicMonitorListener::start).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(mqttFutures).join();
    }

    @Override
    public void close() throws IOException {
        listeners.forEach(MqttTopicMonitorListener::close);
    }
}
