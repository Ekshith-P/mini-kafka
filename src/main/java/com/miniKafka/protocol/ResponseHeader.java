package com.minikafka.protocol;

import java.nio.ByteBuffer;

/**
 * The prefix on every response. Like Kafka, it carries only the correlation id - per-item error
 * codes live inside the response body, not the header.
 */
public final class ResponseHeader {
    public final int correlationId;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buf) {
        return new ResponseHeader(buf.getInt());
    }

    public void writeTo(ByteWriter out) {
        out.putInt(correlationId);
    }
}