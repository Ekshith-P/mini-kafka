package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;

/**
 * HEARTBEAT response. A {@code REBALANCE_IN_PROGRESS} or {@code ILLEGAL_GENERATION} error tells the
 * member the group changed and it must rejoin.
 */
public final class HeartbeatResponse {
    public final short errorCode;

    public HeartbeatResponse(short errorCode) {
        this.errorCode = errorCode;
    }

    public void writeTo(ByteWriter out) {
        out.putShort(errorCode);
    }

    public static HeartbeatResponse parse(ByteBuffer buf) {
        return new HeartbeatResponse(buf.getShort());
    }
}