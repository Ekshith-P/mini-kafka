package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** LIST_OFFSETS response: the resolved offset for each requested partition. */
public final class ListOffsetsResponse {

    public static final class Partition {
        public final int index;
        public final short errorCode;
        public final long timestamp;
        public final long offset;

        public Partition(int index, short errorCode, long timestamp, long offset) {
            this.index = index;
            this.errorCode = errorCode;
            this.timestamp = timestamp;
            this.offset = offset;
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

    public ListOffsetsResponse(List<Topic> topics) {
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
                out.putLong(partition.timestamp);
                out.putLong(partition.offset);
            }
        }
    }

    public static ListOffsetsResponse parse(ByteBuffer buf) {
        int topicCount = buf.getInt();
        List<Topic> topics = new ArrayList<>(Math.max(0, topicCount));
        for (int t = 0; t < topicCount; t++) {
            String name = Protocol.getString(buf);
            int partitionCount = buf.getInt();
            List<Partition> partitions = new ArrayList<>(Math.max(0, partitionCount));
            for (int p = 0; p < partitionCount; p++) {
                int index = buf.getInt();
                short errorCode = buf.getShort();
                long timestamp = buf.getLong();
                long offset = buf.getLong();
                partitions.add(new Partition(index, errorCode, timestamp, offset));
            }
            topics.add(new Topic(name, partitions));
        }
        return new ListOffsetsResponse(topics);
    }
}