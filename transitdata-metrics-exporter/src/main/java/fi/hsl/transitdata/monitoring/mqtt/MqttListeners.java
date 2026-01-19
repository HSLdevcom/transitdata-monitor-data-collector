package fi.hsl.transitdata.monitoring.mqtt;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public record MqttListeners(List<MqttTopicMonitorListener> listeners) implements Closeable {

    public void start() {
        listeners.forEach(MqttTopicMonitorListener::start);
    }

    @Override
    public void close() throws IOException {
        listeners.forEach(MqttTopicMonitorListener::close);
    }
}
