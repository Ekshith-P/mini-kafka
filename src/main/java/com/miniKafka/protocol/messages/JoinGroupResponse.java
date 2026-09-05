package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;

/**
 * JOIN_GROUP response: the assigned member id and the (new) group generation.
 */
public final class JoinGroupResponse {

    public final short errorCode;
    public final int generationId;
    public final String memberId;

    public JoinGroupResponse(
            short errorCode,
            int generationId,
            String memberId
    ) {
        this.errorCode = errorCode;
        this.generationId = generationId;
        this.memberId = memberId;
    }

    public void writeTo(ByteWriter out) {
        out.putShort(errorCode);
        out.putInt(generationId);
        out.putString(memberId);
    }

    public static JoinGroupResponse parse(ByteBuffer buf) {
        return new JoinGroupResponse(
                buf.getShort(),
                buf.getInt(),
                Protocol.getString(buf)
        );
    }
}