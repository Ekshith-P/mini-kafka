package com.minikafka.client;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.RequestHeader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A single blocking TCP connection to one broker that speaks the request/response protocol. It sends
 * one request at a time and waits for the matching reply, verifying the echoed correlation id.
 *
 * <p>Not thread-safe: a connection carries at most one in-flight request, so callers that share one
 * must serialize access (the producer and replica-fetcher each use their own).
 */
public final class BrokerConnection implements Closeable {
    private static final int MAX_RESPONSE_BYTES = 100 * 1024 * 1024;

    private final String host;
    private final int port;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private int correlationCounter;

    public BrokerConnection(String host, int port, int connectTimeoutMs) throws IOException {
        this(host, port, connectTimeoutMs, 0);
    }

    /**
     * @param readTimeoutMs socket read timeout (0 = block forever). Used by the Raft transport so a
     *                      peer that accepts a connection but never replies can't stall a caller.
     */
    public BrokerConnection(String host, int port, int connectTimeoutMs, int readTimeoutMs)
            throws IOException {
        this.host = host;
        this.port = port;
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        if (readTimeoutMs > 0) {
            this.socket.setSoTimeout(readTimeoutMs);
        }
        this.in = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(
                new BufferedOutputStream(socket.getOutputStream()));
    }

    /**
     * Sends a request and returns the response body positioned just after the correlation id.
     *
     * @param clientId free-form identifier for logging on the broker
     * @param body     the api-specific request body (everything after the request header)
     */
    public ByteBuffer send(short apiKey, short apiVersion, String clientId, byte[] body)
            throws IOException {
        int correlationId = ++correlationCounter;

        RequestHeader header =
                new RequestHeader(apiKey, apiVersion, correlationId, clientId);

        ByteWriter w = new ByteWriter(body.length + 16);
        header.writeTo(w);
        w.putRaw(body);
        byte[] payload = w.toByteArray();

        out.writeInt(payload.length);
        out.write(payload);
        out.flush();

        int size = in.readInt();
        if (size <= 0 || size > MAX_RESPONSE_BYTES) {
            throw new IOException("bad response size " + size + " from " + this);
        }

        byte[] resp = new byte[size];
        in.readFully(resp);

        ByteBuffer buf = ByteBuffer.wrap(resp);
        int echoed = buf.getInt();

        if (echoed != correlationId) {
            throw new IOException(
                    "correlation id mismatch: expected " + correlationId + " got " + echoed);
        }

        return buf;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}