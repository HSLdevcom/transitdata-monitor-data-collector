package fi.hsl.transitdata.monitoring.mqtt;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.util.UUID.randomUUID;

public class MqttClientId {

    public static String get() {
        var hostName = hostName();

        return hostName.contains("transitdata-metrics-exporter")
                ? hostName
                : "transitdata-metrics-exporter-" + hostName;
    }

    private static String hostName() {
        var envHostName = System.getenv("HOSTNAME");
        if (envHostName != null && !envHostName.isEmpty()) {
            return envHostName;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-" + randomUUID().toString().substring(0, 8);
        }
    }
}
