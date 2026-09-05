package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * LIST_OFFSETS request: asks for a partition's earliest or latest offset. The special timestamps
 * {@link #EARLIEST} and {@link #LATEST} select the log-start offset and high-watermark respectively,
 * i.e. how a consumer decides where to begin when it has no committed offset.
 */
public final class ListOffsetsRequest {

    public static final long EARLIEST = -2L;
    public static final long LATEST = -1L;

    public static final class Partition {
        public final int index;
        public final long timestamp;

        public Partition(int index, long timestamp) {
            this.index = index;
            this.timestamp = timestamp;
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

    public final int replicaId;
    public final List<Topic> topics;

    public ListOffsetsRequest(int replicaId, List<Topic> topics) {
        this.replicaId = replicaId;
        this.topics = topics;
    }

    public static ListOffsetsRequest parse(ByteBuffer buf) {
        int replicaId = buf.getInt();
        int topicCount = buf.getInt();

        List<Topic> topics =
                new ArrayList<>(Math.max(0, topicCount));

        for (int t = 0; t < topicCount; t++) {
            String name = Protocol.getString(buf);

            int partitionCount = buf.getInt();

            List<Partition> partitions =
                    new ArrayList<>(Math.max(0, partitionCount));

            for (int p = 0; p < partitionCount; p++) {
                int index = buf.getInt();
                long timestamp = buf.getLong();

                partitions.add(
                        new Partition(index, timestamp)
                );
            }

            topics.add(new Topic(name, partitions));
        }

        return new ListOffsetsRequest(replicaId, topics);
    }

    public void writeTo(ByteWriter out) {
        out.putInt(replicaId);
        out.putInt(topics.size());

        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());

            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putLong(partition.timestamp);
            }
        }
    }
}