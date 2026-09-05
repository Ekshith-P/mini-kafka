package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * PRODUCE response: per-partition result of an append. {@code baseOffset} is the offset assigned to
 * the first record of the batch (or -1 on error).
 */
public final class ProduceResponse {

    public static final class Partition {
        public final int index;
        public final short errorCode;
        public final long baseOffset;

        public Partition(int index, short errorCode, long baseOffset) {
            this.index = index;
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
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

    public ProduceResponse(List<Topic> topics) {
        this.topics = topics;
    }

    public void writeTo(ByteWriter out) {
        out.putInt(topics.size());
        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());
            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putShort(partition.errorCode);
                out.putLong(partition.baseOffset);
            }
        }
    }

    public static ProduceResponse parse(ByteBuffer buf) {
        int topicCount = buf.getInt();
        List<Topic> topics = new ArrayList<>(Math.max(0, topicCount));
        for (int t = 0; t < topicCount; t++) {
            String name = Protocol.getString(buf);
            int partitionCount = buf.getInt();
            List<Partition> partitions = new ArrayList<>(Math.max(0, partitionCount));
            for (int p = 0; p < partitionCount; p++) {
                int index = buf.getInt();
                short errorCode = buf.getShort();
                long baseOffset = buf.getLong();
                partitions.add(new Partition(index, errorCode, baseOffset));
            }
            topics.add(new Topic(name, partitions));
        }
        return new ProduceResponse(topics);
    }
}