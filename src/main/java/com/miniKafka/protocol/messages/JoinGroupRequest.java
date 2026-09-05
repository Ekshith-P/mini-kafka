package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * JOIN_GROUP request: a consumer asks to join a group. An empty {@code memberId} means "I'm new,
 * so assign me one"; the coordinator blocks the reply until the rebalance settles, then returns the
 * group's current generation.
 */
public final class JoinGroupRequest {

    public final String groupId;
    public final String memberId; // "" for a new member
    public final int sessionTimeoutMs;
    public final List<String> topics;

    public JoinGroupRequest(
            String groupId,
            String memberId,
            int sessionTimeoutMs,
            List<String> topics
    ) {
        this.groupId = groupId;
        this.memberId = memberId;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.topics = topics;
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putString(memberId);
        out.putInt(sessionTimeoutMs);
        out.putInt(topics.size());

        for (String topic : topics) {
            out.putString(topic);
        }
    }

    public static JoinGroupRequest parse(ByteBuffer buf) {
        String groupId = Protocol.getString(buf);
        String memberId = Protocol.getString(buf);
        int sessionTimeoutMs = buf.getInt();

        int count = buf.getInt();

        List<String> topics =
                new ArrayList<>(Math.max(0, count));

        for (int i = 0; i < count; i++) {
            topics.add(Protocol.getString(buf));
        }

        return new JoinGroupRequest(
                groupId,
                memberId,
                sessionTimeoutMs,
                topics
        );
    }
}