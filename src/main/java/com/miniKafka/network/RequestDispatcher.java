package com.minikafka.network;

import com.minikafka.protocol.RequestHeader;

import java.nio.ByteBuffer;

/**
 * Turns a parsed request into a response body. Implemented by the broker's request handler; the
 * network layer depends only on this narrow interface so it stays decoupled from broker internals.
 *
 * @return the response body bytes (everything after the response header); the network layer prepends
 *         the correlation id and length frame
 */
@FunctionalInterface
public interface RequestDispatcher {
    byte[] dispatch(RequestHeader header, ByteBuffer body);
}