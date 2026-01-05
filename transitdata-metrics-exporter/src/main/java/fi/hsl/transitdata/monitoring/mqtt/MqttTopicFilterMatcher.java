package fi.hsl.transitdata.monitoring.mqtt;

import java.util.Optional;

public class MqttTopicFilterMatcher {

    public static Optional<String> findMatchingTopicFilter(String topic, String[] topicFilters) {
        for (var topicFilter : topicFilters) {
            if (topicMatches(topic, topicFilter)) {
                return Optional.of(topicFilter);
            }
        }
        return Optional.empty();
    }

    private static boolean topicMatches(String topic, String topicFilter) {
        // MQTT topic filter matching rules:
        // + matches single level
        // # matches multiple levels
        if (topicFilter.equals(topic)) {
            return true;
        }

        var filterParts = topicFilter.split("/");
        var topicParts = topic.split("/");

        if (filterParts.length != topicParts.length && !topicFilter.contains("#")) {
            return false;
        }

        for (int i = 0; i < filterParts.length; i++) {
            if (filterParts[i].equals("#")) {
                return true; // # matches everything after this point
            }

            if (i >= topicParts.length) {
                return false;
            }

            if (!filterParts[i].equals("+") && !filterParts[i].equals(topicParts[i])) {
                return false;
            }
        }

        return topicParts.length == filterParts.length;
    }
}
