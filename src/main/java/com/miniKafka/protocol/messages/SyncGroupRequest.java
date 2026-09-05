package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;

/** SYNC_GROUP request: after joining, a member asks the coordinator for its partition assignment. */
public final class SyncGroupRequest {
    public final String groupId;
    public final String memberId;
    public final int generationId;

    public SyncGroupRequest(String groupId, String memberId, int generationId) {
        this.groupId = groupId;
        this.memberId = memberId;
        this.generationId = generationId;
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putString(memberId);
        out.putInt(generationId);
    }

    public static SyncGroupRequest parse(ByteBuffer buf) {
        return new SyncGroupRequest(Protocol.getString(buf), Protocol.getString(buf), buf.getInt());
    }
}