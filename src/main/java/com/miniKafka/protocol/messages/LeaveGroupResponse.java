package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;

/** LEAVE_GROUP response. */
public final class LeaveGroupResponse {
    public final short errorCode;

    public LeaveGroupResponse(short errorCode) {
        this.errorCode = errorCode;
    }

    public void writeTo(ByteWriter out) {
        out.putShort(errorCode);
    }

    public static LeaveGroupResponse parse(ByteBuffer buf) {
        return new LeaveGroupResponse(buf.getShort());
    }
}