package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** OFFSET_FETCH response: the committed offset per partition ({@code -1} if the group has none). */
public final class OffsetFetchResponse {

    public static final class Partition {
        public final int index;
        public final long committedOffset;
        public final String metadata; // nullable
        public final short errorCode;

        public Partition(int index, long committedOffset, String metadata, short errorCode) {
            this.index = index;
            this.committedOffset = committedOffset;
            this.metadata = metadata;
            this.errorCode = errorCode;
        }
    }

    public static final class Topic {
        public final String name;
        public final List<Partition> partitions;

        public Topic(String name, List<Partition> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }

    public final List<Topic> topics;

    public OffsetFetchResponse(List<Topic> topics) {
        this.topics = topics;
    }

    public void writeTo(ByteWriter out) {
        out.putInt(topics.size());
        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());
            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putLong(partition.committedOffset);
                out.putString(partition.metadata);
                out.putShort(partition.errorCode);
            }
        }
    }

    public static OffsetFetchResponse parse(ByteBuffer buf) {
        int topicCount = buf.getInt();
        List<Topic> topics = new ArrayList<>(Math.max(0, topicCount));
        for (int t = 0; t < topicCount; t++) {
            String name = Protocol.getString(buf);
            int partitionCount = buf.getInt();
            List<Partition> partitions = new ArrayList<>(Math.max(0, partitionCount));
            for (int p = 0; p < partitionCount; p++) {
                int index = buf.getInt();
                long committedOffset = buf.getLong();
                String metadata = Protocol.getString(buf);
                short errorCode = buf.getShort();
                partitions.add(new Partition(index, committedOffset, metadata, errorCode));
            }
            topics.add(new Topic(name, partitions));
        }
        return new OffsetFetchResponse(topics);
    }
}