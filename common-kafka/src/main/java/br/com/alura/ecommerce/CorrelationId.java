package br.com.alura.ecommerce;

import java.util.UUID;

public class CorrelationId {
    private final String id;

    CorrelationId(String title) {
        id = title + "(" + UUID.randomUUID() + ")";
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "{\"CorrelationId\":{"
                + "\"id\":\"" + id + "\""
                + "}}";
    }

    public CorrelationId continueWith(String newTitle) {
        return new CorrelationId(id + "-" + newTitle);
    }
}
