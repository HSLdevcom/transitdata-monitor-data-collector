package fi.hsl.transitdata.monitoring;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

record AppConfig(int port, List<String> gtfsRtUrls, Duration gtfsRtPollInterval, Duration gtfsRtClientTimeout) {

    public static AppConfig parseFrom(String configurationFile) {
        var config = ConfigFactory.parseResources(configurationFile).resolve();
        return buildFrom(config);
    }

    private static AppConfig buildFrom(Config config) {
        var port = getRequired(config, "port", config::getInt);
        var gtfsRtUrls = List.of((getRequired(config, "gtfsrt.urls", config::getString)).split(","));
        var gtfsRtPollInterval = Duration.parse(getRequired(config, "gtfsrt.pollInterval", config::getString));
        var gtfsRtClientTimeout = Duration.parse(getRequired(config, "gtfsrt.clientTimeout", config::getString));

        return new AppConfig(port, gtfsRtUrls, gtfsRtPollInterval, gtfsRtClientTimeout);
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
