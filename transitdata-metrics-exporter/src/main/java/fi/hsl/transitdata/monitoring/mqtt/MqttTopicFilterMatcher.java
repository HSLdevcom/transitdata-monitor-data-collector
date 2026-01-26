package fi.hsl.transitdata.monitoring.mqtt;

import java.util.ArrayList;
import java.util.List;

public class MqttTopicFilterMatcher {

    public static List<String> findMatchingTopicFilters(String topic, String[] topicFilters) {
        var matches = new ArrayList<String>();
        for (var topicFilter : topicFilters) {
            if (topicMatches(topic, topicFilter)) {
                matches.add(topicFilter);
            }
        }
        return matches;
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
