package com.minikafka.protocol;

import java.nio.ByteBuffer;

/**
 * The fixed prefix on every request:
 * <pre>
 * apiKey        INT16           which request this is (see {@link com.minikafka.common.ApiKeys})
 * apiVersion    INT16           version of that request's schema
 * correlationId INT32           echoed back in the response so a client can match replies to requests
 * clientId      NULLABLE_STRING free-form client identifier (for logging)
 * </pre>
 */
public final class RequestHeader {
    public final short apiKey;
    public final short apiVersion;
    public final int correlationId;
    public final String clientId;

    public RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public static RequestHeader parse(ByteBuffer buf) {
        short apiKey = buf.getShort();
        short apiVersion = buf.getShort();
        int correlationId = buf.getInt();
        String clientId = Protocol.getString(buf);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }

    public void writeTo(ByteWriter out) {
        out.putShort(apiKey);
        out.putShort(apiVersion);
        out.putInt(correlationId);
        out.putString(clientId);
    }

    @Override
    public String toString() {
        return "RequestHeader{apiKey=" + apiKey + ", apiVersion=" + apiVersion
                + ", correlationId=" + correlationId + ", clientId='" + clientId + "'}";
    }
}