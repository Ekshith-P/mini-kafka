package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** CREATE_TOPICS request: create one or more topics with a partition count and replication factor. */
public final class CreateTopicsRequest {

    public static final class Topic {
        public final String name;
        public final int numPartitions;
        public final short replicationFactor;

        public Topic(String name, int numPartitions, short replicationFactor) {
            this.name = name;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }
    }

    public final List<Topic> topics;
    public final int timeoutMs;

    public CreateTopicsRequest(List<Topic> topics, int timeoutMs) {
        this.topics = topics;
        this.timeoutMs = timeoutMs;
    }

    public static CreateTopicsRequest parse(ByteBuffer buf) {
        int count = buf.getInt();
        List<Topic> topics = new ArrayList<>(Math.max(0, count));

        for (int i = 0; i < count; i++) {
            String name = Protocol.getString(buf);
            int numPartitions = buf.getInt();
            short replicationFactor = buf.getShort();

            topics.add(new Topic(name, numPartitions, replicationFactor));
        }

        int timeoutMs = buf.getInt();

        return new CreateTopicsRequest(topics, timeoutMs);
    }

    public void writeTo(ByteWriter out) {
        out.putInt(topics.size());

        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.numPartitions);
            out.putShort(topic.replicationFactor);
        }

        out.putInt(timeoutMs);
    }
}