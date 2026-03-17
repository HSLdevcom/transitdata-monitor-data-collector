package fi.hsl.transitdata.monitoring;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppConfigTest {

    @Test
    void shouldParseValidConfiguration() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                    brokers = [
                        {
                            address = "tcp://mqtt.example.com:1883"
                            topicFilters = ["/hfp/v2/journey/#"]
                        }
                    ]
                    qos = 0
                }
                """;

        // when
        var config = parseConfig(configString);

        // then
        assertThat(config.port()).isEqualTo(8080);
        assertThat(config.gtfsRtUrls()).containsExactly("http://example.com/gtfsrt");
        assertThat(config.gtfsRtPollInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.gtfsRtClientTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.mqttClientId()).isEqualTo("test-client");
        assertThat(config.mqttConnectionTimeout()).isEqualTo(Duration.ofSeconds(15));
        assertThat(config.mqttKeepAliveInterval()).isEqualTo(Duration.ofSeconds(20));
        assertThat(config.mqttBrokers()).hasSize(1);
        assertThat(config.mqttBrokers().getFirst().address()).isEqualTo("tcp://mqtt.example.com:1883");
        assertThat(config.mqttBrokers().getFirst().topicFilters()).containsExactly("/hfp/v2/journey/#");
    }

    @Test
    void shouldAllowEmptyMqttBrokersList() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                    qos = 0
                }
                """;

        // when
        var config = parseConfig(configString);

        // then
        assertThat(config.mqttBrokers()).isEmpty();
    }

    @Test
    void shouldParseMultipleGtfsRtUrls() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/vp", "http://example.com/tu", "http://example.com/sa"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                    qos = 0
                }
                """;

        // when
        var config = parseConfig(configString);

        // then
        assertThat(config.gtfsRtUrls()).containsExactly("http://example.com/vp", "http://example.com/tu",
                "http://example.com/sa");
    }

    @Test
    void shouldParseMultipleMqttBrokers() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                    qos = 0
                    brokers = [
                        {
                            address = "tcp://mqtt1.example.com:1883"
                            topicFilters = ["/hfp/v2/journey/#"]
                        },
                        {
                            address = "tcp://mqtt2.example.com:1883"
                            topicFilters = ["gtfsrt/v2/fi/hsl/#", "gtfsrt/dev/fi/hsl/#"]
                        }
                    ]
                }
                """;

        // when
        var config = parseConfig(configString);

        // then
        assertThat(config.mqttBrokers()).hasSize(2);
        assertThat(config.mqttBrokers().get(0).address()).isEqualTo("tcp://mqtt1.example.com:1883");
        assertThat(config.mqttBrokers().get(0).topicFilters()).containsExactly("/hfp/v2/journey/#");
        assertThat(config.mqttBrokers().get(1).address()).isEqualTo("tcp://mqtt2.example.com:1883");
        assertThat(config.mqttBrokers().get(1).topicFilters()).containsExactly("gtfsrt/v2/fi/hsl/#",
                "gtfsrt/dev/fi/hsl/#");
    }

    @Test
    void shouldRejectPollIntervalShorterThanClientTimeout() {
        // given - poll interval (5s) is shorter than client timeout (30s)
        // This would cause overlapping requests: a slow request could still be in progress
        // when the next poll starts, leading to resource exhaustion.
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT5S"
                    clientTimeout = "PT30S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                }
                """;

        // when / then
        assertThatThrownBy(() -> parseConfig(configString)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gtfsrt.pollInterval")
                .hasMessageContaining("must be longer than")
                .hasMessageContaining("gtfsrt.clientTimeout");
    }

    @Test
    void shouldRejectPollIntervalEqualToClientTimeout() {
        // given - poll interval equals client timeout
        // Even with equal values, a request timing out exactly at the poll interval boundary
        // could cause the next poll to start while cleanup is still happening.
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT10S"
                    clientTimeout = "PT10S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                }
                """;

        // when / then
        assertThatThrownBy(() -> parseConfig(configString)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gtfsrt.pollInterval")
                .hasMessageContaining("must be longer than")
                .hasMessageContaining("gtfsrt.clientTimeout");
    }

    @Test
    void shouldAcceptPollIntervalLongerThanClientTimeout() {
        // given - poll interval (30s) is longer than client timeout (5s)
        // This ensures a request will always complete (or timeout) before the next poll starts.
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                    qos = 0
                }
                """;

        // when
        var config = parseConfig(configString);

        // then
        assertThat(config.gtfsRtPollInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.gtfsRtClientTimeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void shouldThrowWhenPortIsMissing() {
        // given
        var configString = """
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                }
                """;

        // when / then
        assertThatThrownBy(() -> parseConfig(configString)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port")
                .hasMessageContaining("required");
    }

    @Test
    void shouldThrowWhenGtfsRtUrlsIsMissing() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    clientId = "test-client"
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                }
                """;

        // when / then
        assertThatThrownBy(() -> parseConfig(configString)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gtfsrt.urls")
                .hasMessageContaining("required");
    }

    @Test
    void shouldThrowWhenMqttClientIdIsMissing() {
        // given
        var configString = """
                port = 8080
                gtfsrt {
                    urls = ["http://example.com/gtfsrt"]
                    pollInterval = "PT30S"
                    clientTimeout = "PT5S"
                }
                mqtt {
                    connectionTimeout = "PT15S"
                    keepAliveInterval = "PT20S"
                }
                """;

        // when / then
        assertThatThrownBy(() -> parseConfig(configString)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mqtt.clientId")
                .hasMessageContaining("required");
    }

    private static AppConfig parseConfig(String configString) {
        var config = ConfigFactory.parseString(configString);
        return AppConfig.buildFrom(config);
    }
}
