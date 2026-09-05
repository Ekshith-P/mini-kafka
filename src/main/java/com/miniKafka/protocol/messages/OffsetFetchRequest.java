package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * OFFSET_FETCH request: a consumer group asks for its last committed offset on each partition.
 */
public final class OffsetFetchRequest {

    public static final class Topic {
        public final String name;
        public final List<Integer> partitions;

        public Topic(String name, List<Integer> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
    }

    public final String groupId;
    public final List<Topic> topics;

    public OffsetFetchRequest(String groupId, List<Topic> topics) {
        this.groupId = groupId;
        this.topics = topics;
    }

    public static OffsetFetchRequest parse(ByteBuffer buf) {
        String groupId = Protocol.getString(buf);

        int topicCount = buf.getInt();
        List<Topic> topics =
                new ArrayList<>(Math.max(0, topicCount));

        for (int t = 0; t < topicCount; t++) {
            String name = Protocol.getString(buf);

            int partitionCount = buf.getInt();
            List<Integer> partitions =
                    new ArrayList<>(Math.max(0, partitionCount));

            for (int p = 0; p < partitionCount; p++) {
                partitions.add(buf.getInt());
            }

            topics.add(new Topic(name, partitions));
        }

        return new OffsetFetchRequest(groupId, topics);
    }

    public void writeTo(ByteWriter out) {
        out.putString(groupId);
        out.putInt(topics.size());

        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());

            for (int partition : topic.partitions) {
                out.putInt(partition);
            }
        }
    }
}