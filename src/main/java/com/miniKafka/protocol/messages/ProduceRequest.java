package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * PRODUCE request: a client asking the leader to append records to one or more partitions.
 *
 * <pre>
 * acks       INT16   0 = no ack, 1 = leader wrote locally, -1 = all in-sync replicas wrote
 * timeoutMs  INT32   how long the broker may wait for acks before timing out
 * topics     ARRAY of Topic
 * </pre>
 */
public final class ProduceRequest {

    public static final class Record {
        public final byte[] key;    // nullable
        public final byte[] value;  // nullable

        public Record(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    public static final class Partition {
        public final int index;
        public final List<Record> records;

        public Partition(int index, List<Record> records) {
            this.index = index;
            this.records = records;
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

    public final short acks;
    public final int timeoutMs;
    public final List<Topic> topics;

    public ProduceRequest(short acks, int timeoutMs, List<Topic> topics) {
        this.acks = acks;
        this.timeoutMs = timeoutMs;
        this.topics = topics;
    }

    public static ProduceRequest parse(ByteBuffer buf) {
        short acks = buf.getShort();
        int timeoutMs = buf.getInt();

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

                int recordCount = buf.getInt();
                List<Record> records =
                        new ArrayList<>(Math.max(0, recordCount));

                for (int r = 0; r < recordCount; r++) {
                    byte[] key = Protocol.getBytes(buf);
                    byte[] value = Protocol.getBytes(buf);
                    records.add(new Record(key, value));
                }

                partitions.add(new Partition(index, records));
            }

            topics.add(new Topic(name, partitions));
        }

        return new ProduceRequest(acks, timeoutMs, topics);
    }

    public void writeTo(ByteWriter out) {
        out.putShort(acks);
        out.putInt(timeoutMs);
        out.putInt(topics.size());

        for (Topic topic : topics) {
            out.putString(topic.name);
            out.putInt(topic.partitions.size());

            for (Partition partition : topic.partitions) {
                out.putInt(partition.index);
                out.putInt(partition.records.size());

                for (Record record : partition.records) {
                    out.putBytes(record.key);
                    out.putBytes(record.value);
                }
            }
        }
    }
}