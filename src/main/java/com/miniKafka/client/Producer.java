package com.minikafka.client;

import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.common.Utils;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.MetadataResponse;
import com.minikafka.protocol.messages.ProduceRequest;
import com.minikafka.protocol.messages.ProduceResponse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * A minimal producer. It fetches metadata to learn a topic's partitions and their leaders, chooses a
 * partition (murmur2 hash of the key, or round-robin when there is no key), sends a PRODUCE request
 * to the leader, and retries on leader changes or transient I/O errors after refreshing metadata.
 */
public final class Producer implements Closeable {
    private static final int MAX_RETRIES = 5;

    public static final class RecordMetadata {
        public final int partition;
        public final long offset;

        public RecordMetadata(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    private final KafkaClient client;
    private final short acks;
    private final int timeoutMs;
    private int roundRobin;

    public Producer(String bootstrapServers, short acks) {
        this.client = new KafkaClient(bootstrapServers, "mini-kafka-producer", 5000);
        this.acks = acks;
        this.timeoutMs = 30000;
    }

    public RecordMetadata send(String topic, String key, String value) throws IOException {
        byte[] keyBytes =
                key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes =
                value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return send(topic, keyBytes, valueBytes);
    }

    public RecordMetadata send(String topic, byte[] key, byte[] value) throws IOException {
        IOException lastIo = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                MetadataResponse md = ensureMetadata(topic);
                MetadataResponse.TopicMetadata tm = topicMetadata(md, topic);

                if (tm == null || tm.partitions.isEmpty()) {
                    throw new IOException("no metadata for topic " + topic);
                }

                int numPartitions = tm.partitions.size();

                int partition = key != null
                        ? Utils.partitionForKey(key, numPartitions)
                        : (roundRobin++ & Integer.MAX_VALUE) % numPartitions;

                int leader = md.leaderFor(topic, partition);

                if (leader < 0) {
                    client.fetchMetadata(Collections.singletonList(topic));
                    continue;
                }

                ProduceRequest.Record record =
                        new ProduceRequest.Record(key, value);

                ProduceRequest.Partition part =
                        new ProduceRequest.Partition(
                                partition,
                                Collections.singletonList(record));

                ProduceRequest.Topic reqTopic =
                        new ProduceRequest.Topic(
                                topic,
                                Collections.singletonList(part));

                ProduceRequest request =
                        new ProduceRequest(
                                acks,
                                timeoutMs,
                                Collections.singletonList(reqTopic));

                ByteWriter body = new ByteWriter();
                request.writeTo(body);

                ByteBuffer buf = client.sendToNode(
                        leader,
                        ApiKeys.PRODUCE.id,
                        body.toByteArray());

                ProduceResponse resp = ProduceResponse.parse(buf);
                ProduceResponse.Partition result =
                        resp.topics.get(0).partitions.get(0);

                if (result.errorCode == Errors.NONE) {
                    return new RecordMetadata(partition, result.baseOffset);
                }

                if (result.errorCode == Errors.NOT_LEADER_FOR_PARTITION
                        || result.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    client.fetchMetadata(Collections.singletonList(topic));
                    sleepBackoff(attempt);
                    continue;
                }

                throw new IOException(
                        "produce failed: " + Errors.message(result.errorCode));

            } catch (IOException e) {
                lastIo = e;
                try {
                    client.fetchMetadata(Collections.singletonList(topic));
                } catch (IOException ignored) {
                    // will retry
                }
                sleepBackoff(attempt);
            }
        }

        throw new IOException(
                "produce to " + topic + " failed after "
                        + MAX_RETRIES + " attempts",
                lastIo);
    }

    private MetadataResponse ensureMetadata(String topic) throws IOException {
        MetadataResponse md = client.currentMetadata();

        if (md == null || topicMetadata(md, topic) == null) {
            md = client.fetchMetadata(Collections.singletonList(topic));
        }

        return md;
    }

    private static MetadataResponse.TopicMetadata topicMetadata(
            MetadataResponse md, String topic) {

        for (MetadataResponse.TopicMetadata tm : md.topics) {
            if (tm.name.equals(topic) && tm.errorCode == Errors.NONE) {
                return tm;
            }
        }

        return null;
    }

    private static void sleepBackoff(int attempt) {
        try {
            Thread.sleep(Math.min(1000, 100L * (attempt + 1)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        client.close();
    }
}