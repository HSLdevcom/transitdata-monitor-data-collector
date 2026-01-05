package fi.hsl.transitdata.monitoring.mqtt;

import java.util.List;

public record MqttBrokerConfig(String address, int port, List<String> topicFilters) {
}
