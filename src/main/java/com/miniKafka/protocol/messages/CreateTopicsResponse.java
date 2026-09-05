package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** CREATE_TOPICS response: per-topic result of the create request. */
public final class CreateTopicsResponse {

    public static final class Topic {
        public final String name;
        public final short errorCode;

        public Topic(String name, short errorCode) {
            this.name = name;
            this.errorCode = errorCode;
        }
    }

    public final List<Topic> topics;

    public CreateTopicsResponse(List<Topic> topics) {
        this.topics = topics;
    }

    public void writeTo(ByteWriter out) {
        out.putInt(topics.size());
        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putShort(topic.errorCode);
        }
    }

    public static CreateTopicsResponse parse(ByteBuffer buf) {
        int count = buf.getInt();
        List<Topic> topics = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            String name = Protocol.getString(buf);
            short errorCode = buf.getShort();
            topics.add(new Topic(name, errorCode));
        }
        return new CreateTopicsResponse(topics);
    }
}