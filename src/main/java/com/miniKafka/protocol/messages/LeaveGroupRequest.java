package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;

/** LEAVE_GROUP request: a member cleanly leaves, letting the coordinator rebalance immediately. */
public final class LeaveGroupRequest {
    public final String groupId;
    public final String memberId;

    public LeaveGroupRequest(String groupId, String memberId) {
        this.groupId = groupId;
        this.memberId = memberId;
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putString(memberId);
    }

    public static LeaveGroupRequest parse(ByteBuffer buf) {
        return new LeaveGroupRequest(Protocol.getString(buf), Protocol.getString(buf));
    }
}