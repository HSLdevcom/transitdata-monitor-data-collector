package fi.hsl.transitdata.monitoring.mqtt;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MqttTopicFilterMatcherTest {

    @Test
    void shouldMatchExactGtfsRtTopic() {
        // given
        var topic = "gtfsrt/v2/fi/hsl/tu";
        var filters = new String[]{"gtfsrt/v2/fi/hsl/tu"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("gtfsrt/v2/fi/hsl/tu");
    }

    @Test
    void shouldMatchHfpJourneyWithMultiLevelWildcard() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022/01216/2107/1/Tapiola/11:06/2265203/5/60;24/18/80/57";
        var filters = new String[]{"/hfp/v2/journey/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/#");
    }

    @Test
    void shouldMatchHfpApcTopic() {
        // given
        var topic = "/hfp/v2/journey/ongoing/apc/bus/0055/01234";
        var filters = new String[]{"/hfp/v2/journey/ongoing/apc/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/apc/#");
    }

    @Test
    void shouldMatchFerryTopicWithSingleLevelWildcard() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/ferry/1019/suomenlinna";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/ferry/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/+/ferry/#");
    }

    @Test
    void shouldMatchMetroTopicWithSingleLevelWildcard() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/metro/1300M1/itakeskus";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/metro/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/+/metro/#");
    }

    @Test
    void shouldMatchSpecificRouteWithMultipleSingleLevelWildcards() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022/01216/7280/1/Tapiola/11:06/2265203";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/+/+/+/7280/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/+/+/+/+/7280/#");
    }

    @Test
    void shouldMatchGtfsRtDevVehiclePositions() {
        // given
        var topic = "gtfsrt/dev/fi/hsl/vp/bus/0022";
        var filters = new String[]{"gtfsrt/dev/fi/hsl/vp/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("gtfsrt/dev/fi/hsl/vp/#");
    }

    @Test
    void shouldMatchGtfsRtServiceAlerts() {
        // given
        var topic = "gtfsrt/dev/fi/hsl/sa";
        var filters = new String[]{"gtfsrt/dev/fi/hsl/sa"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("gtfsrt/dev/fi/hsl/sa");
    }

    @Test
    void shouldReturnFirstMatchingFilterFromMultiple() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/ferry/1019";
        var filters = new String[]{
                "/hfp/v2/journey/#",
                "/hfp/v2/journey/ongoing/+/ferry/#",
                "/hfp/v2/journey/ongoing/+/metro/#"
        };

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/#");
    }

    @Test
    void shouldNotMatchBusTopicWithFerryFilter() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022/01216";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/ferry/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldNotMatchMetroTopicWithFerryFilter() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/metro/1300M1";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/ferry/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldNotMatchWrongRoute() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022/01216/550/1/Itakeskus";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/+/+/+/7280/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldNotMatchGtfsRtProdWithDevFilter() {
        // given
        var topic = "gtfsrt/v2/fi/hsl/tu";
        var filters = new String[]{"gtfsrt/dev/fi/hsl/tu"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnEmptyWhenFiltersArrayIsEmpty() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022";
        var filters = new String[]{};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldMatchTopicWithLeadingSlash() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022";
        var filters = new String[]{"/hfp/v2/journey/ongoing/+/bus/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/+/bus/#");
    }

    @Test
    void shouldMatchApcTopicSpecifically() {
        // given
        var topic = "/hfp/v2/journey/ongoing/apc/bus/0055/01234/2107/1/Tapiola";
        var filters = new String[]{
                "/hfp/v2/journey/ongoing/apc/#",
                "/hfp/v2/journey/#"
        };

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/ongoing/apc/#");
    }

    @Test
    void shouldMatchComplexBusTopic() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022/01216/2107/1/Tapiola/11:06/2265203/5/60;24/18/80/57";
        var filters = new String[]{
                "/hfp/v2/journey/ongoing/apc/#",
                "/hfp/v2/journey/ongoing/+/ferry/#",
                "/hfp/v2/journey/#"
        };

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("/hfp/v2/journey/#");
    }

    @Test
    void shouldNotMatchApcWithGeneralJourneyFilter() {
        // given
        var topic = "/hfp/v2/journey/ongoing/apc/bus/0055";
        var filters = new String[]{"/hfp/v2/journey/ongoing/vp/#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldMatchMultiLevelWildcardAtRoot() {
        // given
        var topic = "/hfp/v2/journey/ongoing/vp/bus/0022";
        var filters = new String[]{"#"};

        // when
        var result = MqttTopicFilterMatcher.findMatchingTopicFilter(topic, filters);

        // then
        assertThat(result).hasValue("#");
    }
}
