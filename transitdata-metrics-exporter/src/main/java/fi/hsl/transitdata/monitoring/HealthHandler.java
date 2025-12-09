package fi.hsl.transitdata.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

class HealthHandler implements HttpHandler {

    private static final byte[] RESPONSE = "OK".getBytes();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, RESPONSE.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(RESPONSE);
        }
    }
}