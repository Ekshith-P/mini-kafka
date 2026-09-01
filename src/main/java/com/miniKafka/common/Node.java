package com.minikafka.common;

import java.util.Objects;

/** Address of a broker in the cluster: a stable numeric id plus a host:port to connect to. */
public final class Node {
    private final int id;
    private final String host;
    private final int port;

    public Node(int id, String host, int port) {
        this.id = id;
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Node)) {
            return false;
        }
        Node node = (Node) o;
        return id == node.id && port == node.port && host.equals(node.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port);
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}