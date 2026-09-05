package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * FETCH request: used by BOTH consumers and follower brokers to read records.
 *
 * <pre>
 * replicaId INT32  -1 for a consumer; a broker id when a follower is replicating
 * maxWaitMs INT32  long-poll budget (how long the broker may wait for minBytes)
 * minBytes INT32   minimum bytes to accumulate before responding
 * topics   ARRAY of Topic
 * </pre>
 *
 * The {@code replicaId} is what lets one API serve two roles: a consumer may only read up to the
 * high-watermark, while a follower may read up to the leader's log-end offset (and its fetch offset
 * tells the leader how far it has replicated).
 */
public final class FetchRequest {

    public static final int CONSUMER_REPLICA_ID = -1;

    public static final class Partition {
        public final int index;
        public final long fetchOffset;
        public final int maxBytes;

        public Partition(int index, long fetchOffset, int maxBytes) {
            this.index = index;
            this.fetchOffset = fetchOffset;
            this.maxBytes = maxBytes;
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
    public final int maxWaitMs;
    public final int minBytes;
    public final List<Topic> topics;

    public FetchRequest(
            int replicaId,
            int maxWaitMs,
            int minBytes,
            List<Topic> topics
    ) {
        this.replicaId = replicaId;
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.topics = topics;
    }

    public boolean isFromFollower() {
        return replicaId >= 0;
    }

    public static FetchRequest parse(ByteBuffer buf) {
        int replicaId = buf.getInt();
        int maxWaitMs = buf.getInt();
        int minBytes = buf.getInt();
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
                long fetchOffset = buf.getLong();
                int maxBytes = buf.getInt();

                partitions.add(
                        new Partition(index, fetchOffset, maxBytes)
                );
            }

            topics.add(new Topic(name, partitions));
        }

        return new FetchRequest(
                replicaId,
                maxWaitMs,
                minBytes,
                topics
        );
    }

    public void writeTo(ByteWriter out) {
        out.putInt(replicaId);
        out.putInt(maxWaitMs);
        out.putInt(minBytes);
        out.putInt(topics.size());

        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());

            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putLong(partition.fetchOffset);
                out.putInt(partition.maxBytes);
            }
        }
    }
}