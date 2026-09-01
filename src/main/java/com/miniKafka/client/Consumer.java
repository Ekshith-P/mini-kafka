package com.minikafka.client;

import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.common.TopicPartition;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.FetchRequest;
import com.minikafka.protocol.messages.FetchResponse;
import com.minikafka.protocol.messages.HeartbeatRequest;
import com.minikafka.protocol.messages.HeartbeatResponse;
import com.minikafka.protocol.messages.JoinGroupRequest;
import com.minikafka.protocol.messages.JoinGroupResponse;
import com.minikafka.protocol.messages.LeaveGroupRequest;
import com.minikafka.protocol.messages.ListOffsetsRequest;
import com.minikafka.protocol.messages.ListOffsetsResponse;
import com.minikafka.protocol.messages.MetadataResponse;
import com.minikafka.protocol.messages.OffsetCommitRequest;
import com.minikafka.protocol.messages.OffsetFetchRequest;
import com.minikafka.protocol.messages.OffsetFetchResponse;
import com.minikafka.protocol.messages.SyncGroupRequest;
import com.minikafka.protocol.messages.SyncGroupResponse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A consumer that participates in a <b>consumer group</b>. On {@link #subscribe} it joins the group,
 * receives its share of the topic's partitions from the coordinator, and heartbeats on a background
 * thread. When the coordinator rebalances (a member joins/leaves/times out) the next heartbeat or
 * poll notices and the consumer rejoins to pick up its new assignment. A group of one gets every
 * partition, so a single consumer behaves like a plain reader.
 *
 * <p>Group requests go to the group's deterministic coordinator broker. Not thread-safe for the
 * caller's use of {@link #poll}/{@link #commitSync}; the heartbeat runs on its own connection.
 */
public final class Consumer implements Closeable {
    public static final class ConsumerRecord {
        public final String topic;
        public final int partition;
        public final long offset;
        public final long timestamp;
        public final byte[] key;
        public final byte[] value;

        ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        public String valueAsString() {
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        }

        public String keyAsString() {
            return key == null ? null : new String(key, StandardCharsets.UTF_8);
        }
    }

    private final KafkaClient client;
    private final KafkaClient heartbeatClient;
    private final String groupId;
    private final boolean fromBeginning;
    private final int fetchMaxBytes = 1024 * 1024;
    private final int maxWaitMs = 500;
    private final int sessionTimeoutMs = 6000;
    private final int heartbeatIntervalMs = 500;

    private String topic;
    private volatile String memberId = "";
    private volatile int generationId = -1;
    private volatile boolean needsRejoin;
    private volatile boolean running;
    private Thread heartbeatThread;

    private volatile List<Integer> assignedPartitions = new ArrayList<>();
    private private Map<TopicPartition, Long> positions = new HashMap<>();

    public Consumer(String bootstrapServers, String groupId, boolean fromBeginning) {
        this.client = new KafkaClient(bootstrapServers, "mini-kafka-consumer", 5000);
        this.heartbeatClient = new KafkaClient(bootstrapServers, "mini-kafka-consumer-hb", 5000);
        this.groupId = groupId;
        this.fromBeginning = fromBeginning;
    }

    public void subscribe(String topic) throws IOException {
        this.topic = topic;
        client.fetchMetadata(Collections.singletonList(topic));
        running = true;
        rejoin();
        startHeartbeatThread();
    }

    /** Joins (or rejoins) the group and refreshes this consumer's partition assignment. */
    private synchronized void rejoin() throws IOException {
        MetadataResponse md = client.currentMetadata();
        if (md == null) {
            md = client.fetchMetadata(Collections.singletonList(topic));
        }
        int coordinator = coordinatorId(md);

        JoinGroupRequest joinReq = new JoinGroupRequest(groupId, memberId, sessionTimeoutMs,
                Collections.singletonList(topic));
        ByteWriter jb = new ByteWriter();
        joinReq.writeTo(jb);
        JoinGroupResponse joinResp = JoinGroupResponse.parse(
                client.sendToNode(coordinator, ApiKeys.JOIN_GROUP.id, jb.toByteArray()));
        if (joinResp.errorCode != Errors.NONE) {
            needsRejoin = true;
            return; // try again on the next poll
        }
        this.memberId = joinResp.memberId;
        this.generationId = joinResp.generationId;

        SyncGroupRequest syncReq = new SyncGroupRequest(groupId, memberId, generationId);
        ByteWriter sb = new ByteWriter();
        syncReq.writeTo(sb);
        SyncGroupResponse syncResp = SyncGroupResponse.parse(
                client.sendToNode(coordinator, ApiKeys.SYNC_GROUP.id, sb.toByteArray()));
        if (syncResp.errorCode != Errors.NONE) {
            needsRejoin = true;
            return;
        }

        List<Integer> assigned = new ArrayList<>();
        for (SyncGroupResponse.Assignment a : syncResp.assignments) {
            if (a.topic.equals(topic)) {
                for (int p : a.partitions) {
                    assigned.add(p);
                }
            }
        }

        // Keep positions for partitions we still own; initialize newly-assigned ones.
        Map<TopicPartition, Long> newPositions = new HashMap<>();
        for (int partition : assigned) {
            TopicPartition tp = new TopicPartition(topic, partition);
            Long existing = positions.get(tp);
            if (existing != null) {
                newPositions.put(tp, existing);
            } else {
                long committed = longCommittedOffset(md, partition);
                long start = committed >= 0 ? committed
                        : endpointOffset(md, partition, fromBeginning ? ListOffsetsRequest.EARLIEST : ListOffsetsRequest.LATEST);
                newPositions.put(tp, Math.max(0, start));
            }
        }
        assignedPartitions = assigned;
        positions = newPositions;
        needsRejoin = false;
    }

    private void startHeartbeatThread() {
        heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(heartbeatIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (!running) {
                    return;
                }
                try {
                    MetadataResponse md = heartbeatClient.currentMetadata();
                    if (md == null) {
                        md = heartbeatClient.fetchMetadata(Collections.singletonList(topic));
                    }
                    HeartbeatRequest hb = new HeartbeatRequest(groupId, memberId, generationId);
                    ByteWriter body = new ByteWriter();
                    hb.writeTo(body);
                    HeartbeatResponse resp = HeartbeatResponse.parse(
                            heartbeatClient.sendToNode(coordinatorId(md), ApiKeys.HEARTBEAT.id, body.toByteArray()));
                    if (resp.errorCode == Errors.REBALANCE_IN_PROGRESS
                            || resp.errorCode == Errors.ILLEGAL_GENERATION
                            || resp.errorCode == Errors.UNKNOWN_MEMBER_ID) {
                        needsRejoin = true;
                    }
                } catch (IOException e) {
                    try {
                        heartbeatClient.fetchMetadata(Collections.singletonList(topic));
                    } catch (IOException ignored) {
                        // coordinator briefly unreachable; retry next tick
                    }
                }
            }
        }, "mk-consumer-heartbeat-" + groupId);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /** Fetches available records from this consumer's assigned partitions (batched per leader). */
    public List<ConsumerRecord> poll() throws IOException {
        if (needsRejoin) {
            rejoin();
        }
        List<ConsumerRecord> out = new ArrayList<>();
        MetadataResponse md = client.currentMetadata();
        if (md == null) {
            md = client.fetchMetadata(Collections.singletonList(topic));
        }
        boolean needRefresh = false;

        Map<Integer, List<Integer>> byLeader = new TreeMap<>();
        for (int partition : assignedPartitions) {
            int leader = md.leaderFor(topic, partition);
            if (leader < 0) {
                needRefresh = true;
            } else {
                byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(partition);
            }
        }

        for (Map.Entry<Integer, List<Integer>> entry : byLeader.entrySet()) {
            int leader = entry.getKey();
            List<FetchRequest.Partition> parts = new ArrayList<>();
            for (int partition : entry.getValue()) {
                long position = positions.getOrDefault(new TopicPartition(topic, partition), 0L);
                parts.add(new FetchRequest.Partition(partition, position, fetchMaxBytes));
            }
            FetchRequest request = new FetchRequest(FetchRequest.CONSUMER_REPLICA_ID, maxWaitMs, 1,
                    Collections.singletonList(new FetchRequest.Topic(topic, parts)));
            ByteWriter body = new ByteWriter();
            request.writeTo(body);
            try {
                ByteBuffer buf = client.sendToNode(leader, ApiKeys.FETCH.id, body.toByteArray());
                FetchResponse resp = FetchResponse.parse(buf);
                for (FetchResponse.Topic t : resp.topics) {
                    for (FetchResponse.Partition p : t.partitions) {
                        TopicPartition tp = new TopicPartition(t.name, p.index);
                        if (p.errorCode == Errors.NONE) {
                            for (FetchResponse.Record r : p.records) {
                                out.add(new ConsumerRecord(t.name, p.index, r.offset, r.timestamp, r.key, r.value));
                            }
                            if (!p.records.isEmpty()) {
                                positions.put(tp, p.records.get(p.records.size() - 1).offset + 1);
                            }
                        } else if (p.errorCode == Errors.OFFSET_OUT_OF_RANGE) {
                            positions.put(tp, p.logStartOffset);
                        } else {
                            needRefresh = true;
                        }
                    }
                }
            } catch (IOException e) {
                needRefresh = true;
            }
        }

        if (needRefresh) {
            client.fetchMetadata(Collections.singletonList(topic));
        }
        return out;
    }

    public void commitSync() throws IOException {
        MetadataResponse md = client.currentMetadata();
        if (md == null) {
            return;
        }
        List<OffsetCommitRequest.Partition> parts = new ArrayList<>();
        for (int partition : assignedPartitions) {
            long position = positions.getOrDefault(new TopicPartition(topic, partition), 0L);
            parts.add(new OffsetCommitRequest.Partition(partition, position, null));
        }
        OffsetCommitRequest request = new OffsetCommitRequest(groupId,
                Collections.singletonList(new OffsetCommitRequest.Topic(topic, parts)));
        ByteWriter body = new ByteWriter();
        request.writeTo(body);
        client.sendToNode(coordinatorId(md), ApiKeys.OFFSET_COMMIT.id, body.toByteArray());
    }

    private long longCommittedOffset(MetadataResponse md, int partition) {
        OffsetFetchRequest request = new OffsetFetchRequest(groupId, Collections.singletonList(
                new OffsetFetchRequest.Topic(topic, Collections.singletonList(partition))));
        ByteWriter body = new ByteWriter();
        request.writeTo(body);
        try {
            ByteBuffer buf = client.sendToNode(coordinatorId(md), ApiKeys.OFFSET_FETCH.id, body.toByteArray());
            OffsetFetchResponse resp = OffsetFetchResponse.parse(buf);
            return resp.topics.get(0).partitions.get(0).committedOffset;
        } catch (IOException e) {
            // Offsets aren't replicated, so if the coordinator broker is down we can't read them -
            // treat as "no committed offset" and let the reset policy take over.
            return -1;
        }
    }

    private long endpointOffset(MetadataResponse md, int partition, long timestamp) throws IOException {
        int leader = md.leaderFor(topic, partition);
        ListOffsetsRequest request = new ListOffsetsRequest(FetchRequest.CONSUMER_REPLICA_ID,
                Collections.singletonList(new ListOffsetsRequest.Topic(topic,
                        Collections.singletonList(new ListOffsetsRequest.Partition(partition, timestamp)))));
        ByteWriter body = new ByteWriter();
        request.writeTo(body);
        ByteBuffer buf = client.sendToNode(leader, ApiKeys.LIST_OFFSETS.id, body.toByteArray());
        ListOffsetsResponse resp = ListOffsetsResponse.parse(buf);
        return resp.topics.get(0).partitions.get(0).offset;
    }

    /** Deterministically maps a group id to one broker, so a group's coordinator is stable. */
    private int coordinatorId(MetadataResponse md) {
        int[] ids = md.brokers.stream().mapToInt(b -> b.nodeId).sorted().toArray();
        int idx = Math.floorMod(groupId.hashCode(), ids.length);
        return ids[idx];
    }

    public List<Integer> assignedPartitions() {
        return new ArrayList<>(assignedPartitions);
    }

    @Override
    public void close() {
        running = false;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        try {
            MetadataResponse md = client.currentMetadata();
            if (md != null && memberId != null && !memberId.isEmpty()) {
                LeaveGroupRequest leave = new LeaveGroupRequest(groupId, memberId);
                ByteWriter body = new ByteWriter();
                leave.writeTo(body);
                client.sendToNode(coordinatorId(md), ApiKeys.LEAVE_GROUP.id, body.toByteArray());
            }
        } catch (IOException ignored) {
            // best-effort leave
        }
        client.close();
        heartbeatClient.close();
    }
}