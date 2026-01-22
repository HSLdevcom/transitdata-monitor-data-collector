package fi.hsl.transitdata.monitoring.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

public class LivenessEndpoint implements HttpHandler {

    private static final byte[] OK_RESPONSE = "OK".getBytes();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, OK_RESPONSE.length);
        try (var os = exchange.getResponseBody()) {
            os.write(OK_RESPONSE);
        }
    }
}