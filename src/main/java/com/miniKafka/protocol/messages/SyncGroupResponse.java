package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** SYNC_GROUP response: this member's assigned partitions, grouped by topic. */
public final class SyncGroupResponse {

    public static final class Assignment {
        public final String topic;
        public final int[] partitions;

        public Assignment(String topic, int[] partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }
    }

    public final short errorCode;
    public final List<Assignment> assignments;

    public SyncGroupResponse(short errorCode, List<Assignment> assignments) {
        this.errorCode = errorCode;
        this.assignments = assignments;
    }

    public void writeTo(ByteWriter out) {
        out.putShort(errorCode);
        out.putInt(assignments.size());
        for (Assignment a : assignments) {
            out.putString(a.topic);
            out.putIntArray(a.partitions);
        }
    }

    public static SyncGroupResponse parse(ByteBuffer buf) {
        short errorCode = buf.getShort();
        int count = buf.getInt();
        List<Assignment> assignments = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            String topic = Protocol.getString(buf);
            int[] partitions = Protocol.getIntArray(buf);
            assignments.add(new Assignment(topic, partitions));
        }
        return new SyncGroupResponse(errorCode, assignments);
    }
}