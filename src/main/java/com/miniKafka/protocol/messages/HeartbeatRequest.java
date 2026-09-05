package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;

/** HEARTBEAT request: a member tells the coordinator it's still alive at a given generation. */
public final class HeartbeatRequest {
    public final String groupId;
    public final String memberId;
    public final int generationId;

    public HeartbeatRequest(String groupId, String memberId, int generationId) {
        this.groupId = groupId;
        this.memberId = memberId;
        this.generationId = generationId;
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putString(memberId);
        out.putInt(generationId);
    }

    public static HeartbeatRequest parse(ByteBuffer buf) {
        return new HeartbeatRequest(Protocol.getString(buf), Protocol.getString(buf), buf.getInt());
    }
}