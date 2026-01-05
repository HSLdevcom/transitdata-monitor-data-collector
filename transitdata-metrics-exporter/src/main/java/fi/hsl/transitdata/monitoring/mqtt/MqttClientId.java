package fi.hsl.transitdata.monitoring.mqtt;

public class MqttClientId {

    public static String get() {
        var hostname = System.getenv().getOrDefault("HOSTNAME", "unknown");

        return hostname.contains("transitdata-metrics-exporter")
                ? hostname
                : "transitdata-metrics-exporter-" + hostname;
    }
}
