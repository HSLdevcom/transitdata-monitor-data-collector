package fi.hsl.transitdata.monitoring;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public record AppConfig(
        int port,
        List<String> gtfsrtUrls,
        Duration gtfsrtPollInterval
) {

    public static AppConfig parseFrom(String configurationFile) {
        var config = ConfigFactory.parseResources(configurationFile).resolve();
        return buildFrom(config);
    }

    private static AppConfig buildFrom(Config config) {
        var port = getRequired(config, "port", config::getInt);
        var gtfsrtUrls = List.of((getRequired(config, "gtfsrt.urls", config::getString)).split(","));
        var gtfsrtPollInterval = Duration.parse(getRequired(config, "gtfsrt.pollInterval", config::getString));

        return new AppConfig(port, gtfsrtUrls, gtfsrtPollInterval);
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
