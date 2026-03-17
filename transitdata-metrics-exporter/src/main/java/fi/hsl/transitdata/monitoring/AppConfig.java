package fi.hsl.transitdata.monitoring;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.transitdata.monitoring.mqtt.MqttBrokerConfig;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static com.typesafe.config.ConfigValueType.LIST;

public record AppConfig(int port, List<String> gtfsRtUrls, Duration gtfsRtPollInterval, Duration gtfsRtClientTimeout,
        String mqttClientId, Duration mqttConnectionTimeout, Duration mqttKeepAliveInterval, int mqttQos,
        List<MqttBrokerConfig> mqttBrokers) {

    public static AppConfig parseFrom(String configurationFile) {
        var config = ConfigFactory.parseResources(configurationFile).resolve();
        return buildFrom(config);
    }

    static AppConfig buildFrom(Config config) {
        var port = getRequired(config, "port", config::getInt);
        var gtfsRtUrls = parseGtfsRtUrls(config);
        var gtfsRtPollInterval = Duration.parse(getRequired(config, "gtfsrt.pollInterval", config::getString));
        var gtfsRtClientTimeout = Duration.parse(getRequired(config, "gtfsrt.clientTimeout", config::getString));
        validateGtfsRtIntervals(gtfsRtPollInterval, gtfsRtClientTimeout);
        var mqttClientId = getRequired(config, "mqtt.clientId", config::getString);
        var mqttConnectionTimeout = Duration.parse(getRequired(config, "mqtt.connectionTimeout", config::getString));
        var mqttKeepAliveInterval = Duration.parse(getRequired(config, "mqtt.keepAliveInterval", config::getString));
        var mqttQos = getRequired(config, "mqtt.qos", config::getInt);
        validateMqttQos(mqttQos);
        var mqttBrokers = parseMqttBrokers(config);

        return new AppConfig(port, gtfsRtUrls, gtfsRtPollInterval, gtfsRtClientTimeout, mqttClientId,
                mqttConnectionTimeout, mqttKeepAliveInterval, mqttQos, mqttBrokers);
    }

    private static List<String> parseGtfsRtUrls(Config config) {
        if (!config.hasPath("gtfsrt.urls")) {
            throw new IllegalArgumentException("gtfsrt.urls is required");
        }

        if (isList(config, "gtfsrt.urls")) {
            return config.getStringList("gtfsrt.urls");
        } else {
            var urlsJson = config.getString("gtfsrt.urls");
            var parsedConfig = ConfigFactory.parseString("urls = " + urlsJson);
            return parsedConfig.getStringList("urls");
        }
    }

    private static List<MqttBrokerConfig> parseMqttBrokers(Config config) {
        if (!config.hasPath("mqtt.brokers")) {
            return List.of();
        }

        List<? extends Config> brokerConfigs;
        if (isList(config, "mqtt.brokers")) {
            brokerConfigs = config.getConfigList("mqtt.brokers");
        } else {
            var brokersJson = config.getString("mqtt.brokers");
            var parsedConfig = ConfigFactory.parseString("brokers = " + brokersJson);
            brokerConfigs = parsedConfig.getConfigList("brokers");
        }

        return brokerConfigs.stream()
                .map(brokerConfig -> new MqttBrokerConfig(brokerConfig.getString("address"),
                        brokerConfig.getStringList("topicFilters")))
                .toList();
    }

    private static void validateMqttQos(int qos) {
        if (qos < 0 || qos > 2) {
            throw new IllegalArgumentException("mqtt.qos must be 0, 1, or 2, but was " + qos);
        }
    }

    private static void validateGtfsRtIntervals(Duration pollInterval, Duration clientTimeout) {
        if (pollInterval.compareTo(clientTimeout) <= 0) {
            throw new IllegalArgumentException("gtfsrt.pollInterval (%s) must be longer than gtfsrt.clientTimeout (%s)."
                    .formatted(pollInterval, clientTimeout));
        }
    }

    private static boolean isList(Config config, String key) {
        return config.getValue(key).valueType() == LIST;
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
