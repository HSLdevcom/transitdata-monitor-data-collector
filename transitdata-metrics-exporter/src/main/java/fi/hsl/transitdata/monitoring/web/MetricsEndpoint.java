package fi.hsl.transitdata.monitoring.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;

public class MetricsEndpoint implements HttpHandler {

    private final PrometheusMeterRegistry registry;

    public MetricsEndpoint(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var response = registry.scrape();
        exchange.sendResponseHeaders(200, response.getBytes().length);
        try (var os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }
}
