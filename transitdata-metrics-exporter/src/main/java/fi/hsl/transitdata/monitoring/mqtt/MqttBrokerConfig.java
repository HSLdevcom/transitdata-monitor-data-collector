package fi.hsl.transitdata.monitoring.mqtt;

import java.util.List;

public record MqttBrokerConfig(String address, List<String> topicFilters) {
}
