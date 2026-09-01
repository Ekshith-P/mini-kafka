package com.minikafka.client;

import com.minikafka.common.Node;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.MetadataRequest;
import com.minikafka.protocol.messages.MetadataResponse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages connections to the cluster for the producer and consumer. It bootstraps from a list of
 * broker addresses, fetches {@link MetadataResponse} to learn broker ids and partition leaders, and
 * keeps one {@link BrokerConnection} per broker id so requests can be routed to the right leader.
 *
 * <p>Not thread-safe; intended to be owned by a single producer or consumer.
 */
public final class KafkaClient implements Closeable {
    private static final class Endpoint {
        final String host;
        final int port;

        Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private final List<Endpoint> bootstrap = new ArrayList<>();
    private final String clientId;
    private final int connectTimeoutMs;

    private final Map<Integer, BrokerConnection> nodeConnections = new HashMap<>();
    private final Map<Integer, Node> nodes = new HashMap<>();
    private BrokerConnection bootstrapConnection;
    private MetadataResponse metadata;

    /** @param bootstrapServers comma-separated {@code host:port} list */
    public KafkaClient(String bootstrapServers, String clientId, int connectTimeoutMs) {
        for (String server : bootstrapServers.split(",")) {
            String[] hp = server.trim().split(":");
            bootstrap.add(new Endpoint(hp[0].trim(), Integer.parseInt(hp[1].trim())));
        }
        this.clientId = clientId;
        this.connectTimeoutMs = connectTimeoutMs;
    }

    private BrokerConnection bootstrapConnection() throws IOException {
        if (bootstrapConnection != null) {
            return bootstrapConnection;
        }
        IOException last = null;
        for (Endpoint e : bootstrap) {
            try {
                bootstrapConnection = new BrokerConnection(e.host, e.port, connectTimeoutMs);
                return bootstrapConnection;
            } catch (IOException ex) {
                last = ex;
            }
        }
        throw new IOException("could not connect to any bootstrap broker", last);
    }

    public MetadataResponse fetchMetadata(List<String> topics) throws IOException {
        ByteWriter body = new ByteWriter();
        new MetadataRequest(topics).writeTo(body);
        try {
            ByteBuffer buf = bootstrapConnection().send(
                    com.minikafka.common.ApiKeys.METADATA.id, (short) 0, clientId, body.toByteArray());
            metadata = MetadataResponse.parse(buf);
        } catch (IOException e) {
            closeQuietly(bootstrapConnection);
            bootstrapConnection = null;
            throw e;
        }
        for (MetadataResponse.Broker b : metadata.brokers) {
            nodes.put(b.nodeId, new Node(b.nodeId, b.host, b.port));
        }
        return metadata;
    }

    public MetadataResponse currentMetadata() {
        return metadata;
    }

    /** Sends a request to a specific broker id, connecting on demand. */
    public ByteBuffer sendToNode(int nodeId, short apiKey, byte[] body) throws IOException {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IOException("unknown broker id " + nodeId + " (refresh metadata)");
        }
        BrokerConnection conn = nodeConnections.get(nodeId);
        if (conn == null) {
            conn = new BrokerConnection(node.host(), node.port(), connectTimeoutMs);
            nodeConnections.put(nodeId, conn);
        }
        try {
            return conn.send(apiKey, (short) 0, clientId, body);
        } catch (IOException e) {
            closeQuietly(conn);
            nodeConnections.remove(nodeId);
            throw e;
        }
    }

    /** Sends to any reachable broker (for metadata/admin requests that any broker can serve). */
    public ByteBuffer sendToAny(short apiKey, byte[] body) throws IOException {
        try {
            return bootstrapConnection().send(apiKey, (short) 0, clientId, body);
        } catch (IOException e) {
            closeQuietly(bootstrapConnection);
            bootstrapConnection = null;
            throw e;
        }
    }

    @Override
    public void close() {
        closeQuietly(bootstrapConnection);
        for (BrokerConnection conn : nodeConnections.values()) {
            closeQuietly(conn);
        }
        nodeConnections.clear();
    }

    private static void closeQuietly(BrokerConnection conn) {
        if (conn != null) {
            conn.close();
        }
    }
}