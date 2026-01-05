package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class HealthEndpoint implements HttpHandler {

    private static final byte[] READY_RESPONSE = "Ready".getBytes();
    private static final byte[] NOT_READY_RESPONSE = "Not ready".getBytes();

    private final AtomicBoolean isReady = new AtomicBoolean(false);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (isReady.get()) {
            exchange.sendResponseHeaders(200, READY_RESPONSE.length);
            try (var os = exchange.getResponseBody()) {
                os.write(READY_RESPONSE);
            }
        } else {
            exchange.sendResponseHeaders(503, NOT_READY_RESPONSE.length);
            try (var os = exchange.getResponseBody()) {
                os.write(NOT_READY_RESPONSE);
            }
        }
    }

    public void markReady() {
        isReady.set(true);
    }

    public void markNotReady() {
        isReady.set(false);
    }
}
