package com.minikafka.protocol.messages;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * FETCH response: the records read plus the partition's high-watermark and log-start offset,
 * let a consumer know how far it can read and detect if it has fallen off the start of the log.
 */
public final class FetchResponse {

    public static final class Record {
        public final long offset;
        public final long timestamp;
        public final byte[] key;   // nullable
        public final byte[] value; // nullable

        public Record(long offset, long timestamp, byte[] key, byte[] value) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }
    }

    public static final class Partition {
        public final int index;
        public final short errorCode;
        public final long highWatermark;
        public final long logStartOffset;
        public final List<Record> records;

        public Partition(
                int index,
                short errorCode,
                long highWatermark,
                long logStartOffset,
                List<Record> records
        ) {
            this.index = index;
            this.errorCode = errorCode;
            this.highWatermark = highWatermark;
            this.logStartOffset = logStartOffset;
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

    public final List<Topic> topics;

    public FetchResponse(List<Topic> topics) {
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
                out.putLong(partition.highWatermark);
                out.putLong(partition.logStartOffset);
                out.putInt(partition.records.size());

                for (Record record : partition.records) {
                    out.putLong(record.offset);
                    out.putLong(record.timestamp);
                    out.putBytes(record.key);
                    out.putBytes(record.value);
                }
            }
        }
    }

    public static FetchResponse parse(ByteBuffer buf) {
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
                short errorCode = buf.getShort();
                long highWatermark = buf.getLong();
                long logStartOffset = buf.getLong();

                int recordCount = buf.getInt();

                List<Record> records =
                        new ArrayList<>(Math.max(0, recordCount));

                for (int r = 0; r < recordCount; r++) {
                    long offset = buf.getLong();
                    long timestamp = buf.getLong();
                    byte[] key = Protocol.getBytes(buf);
                    byte[] value = Protocol.getBytes(buf);

                    records.add(
                            new Record(offset, timestamp, key, value)
                    );
                }

                partitions.add(
                        new Partition(
                                index,
                                errorCode,
                                highWatermark,
                                logStartOffset,
                                records
                        )
                );
            }

            topics.add(new Topic(name, partitions));
        }

        return new FetchResponse(topics);
    }
}