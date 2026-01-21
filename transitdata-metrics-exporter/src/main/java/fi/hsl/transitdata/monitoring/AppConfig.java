package fi.hsl.transitdata.monitoring;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.transitdata.monitoring.mqtt.MqttBrokerConfig;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public record AppConfig(int port, List<String> gtfsRtUrls, Duration gtfsRtPollInterval, Duration gtfsRtClientTimeout,
        String mqttClientId, Duration mqttConnectionTimeout, Duration mqttKeepAliveInterval,
        List<MqttBrokerConfig> mqttBrokers) {

    public static AppConfig parseFrom(String configurationFile) {
        var config = ConfigFactory.parseResources(configurationFile).resolve();
        return buildFrom(config);
    }

    private static AppConfig buildFrom(Config config) {
        var port = getRequired(config, "port", config::getInt);
        var gtfsRtUrls = parseGtfsRtUrls(config);
        var gtfsRtPollInterval = Duration.parse(getRequired(config, "gtfsrt.pollInterval", config::getString));
        var gtfsRtClientTimeout = Duration.parse(getRequired(config, "gtfsrt.clientTimeout", config::getString));
        var mqttClientId = getRequired(config, "mqtt.clientId", config::getString);
        var mqttConnectionTimeout = Duration.parse(getRequired(config, "mqtt.connectionTimeout", config::getString));
        var mqttKeepAliveInterval = Duration.parse(getRequired(config, "mqtt.keepAliveInterval", config::getString));
        var mqttBrokers = parseMqttBrokers(config);

        return new AppConfig(port, gtfsRtUrls, gtfsRtPollInterval, gtfsRtClientTimeout, mqttClientId,
                mqttConnectionTimeout, mqttKeepAliveInterval, mqttBrokers);
    }

    private static List<String> parseGtfsRtUrls(Config config) {
        if (!config.hasPath("gtfsrt.urls")) {
            throw new IllegalArgumentException("gtfsrt.urls is required");
        }

        var urlsJson = config.getString("gtfsrt.urls");
        var parsedConfig = ConfigFactory.parseString("urls = " + urlsJson);
        return parsedConfig.getStringList("urls");
    }

    private static List<MqttBrokerConfig> parseMqttBrokers(Config config) {
        if (!config.hasPath("mqtt.brokers")) {
            return List.of();
        }

        var brokersJson = config.getString("mqtt.brokers");
        var parsedConfig = ConfigFactory.parseString("brokers = " + brokersJson);
        return parsedConfig.getConfigList("brokers")
                .stream()
                .map(brokerConfig -> new MqttBrokerConfig(brokerConfig.getString("address"),
                        brokerConfig.getStringList("topicFilters")))
                .toList();
    }

    private static <T> T getRequired(Config config, String path, Function<String, T> f) {
        var value = config.hasPath(path) ? f.apply(path) : null;
        checkRequired(path, value);
        return value;
    }

    private static void checkRequired(String paramName, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(paramName + " is required");
        }
    }
}
