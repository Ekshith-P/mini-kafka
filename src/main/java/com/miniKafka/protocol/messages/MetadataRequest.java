package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * METADATA request: asks the broker which brokers exist and, for each requested topic, which broker
 * leads each partition. A null/empty topic list means "all topics". Clients call this first so they
 * know where to route produce and fetch requests.
 */
public final class MetadataRequest {

    // null means "all topics"
    public final List<String> topics;

    public MetadataRequest(List<String> topics) {
        this.topics = topics;
    }

    public static MetadataRequest parse(ByteBuffer buf) {
        int count = buf.getInt();

        if (count < 0) {
            return new MetadataRequest(null);
        }

        List<String> topics = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            topics.add(Protocol.getString(buf));
        }

        return new MetadataRequest(topics);
    }

    public void writeTo(ByteWriter out) {
        if (topics == null) {
            out.putInt(-1);
            return;
        }

        out.putInt(topics.size());

        for (String topic : topics) {
            out.putString(topic);
        }
    }
}