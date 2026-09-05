package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * OFFSET_COMMIT request: a consumer group records how far it has processed each partition, so it can
 * resume from there after a restart. Here the committed offsets are stored by the broker in a simple
 * durable map (real Kafka stores them in the internal {@code __consumer_offsets} topic).
 */
public final class OffsetCommitRequest {

    public static final class Partition {
        public final int index;
        public final long committedOffset;
        public final String metadata; // nullable

        public Partition(int index, long committedOffset, String metadata) {
            this.index = index;
            this.committedOffset = committedOffset;
            this.metadata = metadata;
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

    public final String groupId;
    public final List<Topic> topics;

    public OffsetCommitRequest(String groupId, List<Topic> topics) {
        this.groupId = groupId;
        this.topics = topics;
    }

    public static OffsetCommitRequest parse(ByteBuffer buf) {
        String groupId = Protocol.getString(buf);
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
                partitions.add(new Partition(index, committedOffset, metadata));
            }
            topics.add(new Topic(name, partitions));
        }
        return new OffsetCommitRequest(groupId, topics);
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putInt(topics.size());
        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());
            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putLong(partition.committedOffset);
                out.putString(partition.metadata);
            }
        }
    }
}