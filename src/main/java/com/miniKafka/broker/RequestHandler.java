package com.minikafka.broker;

import com.minikafka.client.BrokerConnection;
import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;
import com.minikafka.metrics.Metrics;
import com.minikafka.network.RequestDispatcher;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.RequestHeader;
import com.minikafka.protocol.messages.*;
import com.minikafka.raft.AppendEntriesRequest;
import com.minikafka.raft.AppendEntriesResponse;
import com.minikafka.raft.RaftNode;
import com.minikafka.raft.RequestVoteRequest;
import com.minikafka.raft.RequestVoteResponse;
import com.minikafka.replication.ReplicaManager;
import com.minikafka.storage.Log;
import com.minikafka.storage.LogReadResult;
import com.minikafka.storage.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parses each request and routes it to storage, replication, or offset state, then serializes the
 * response body. This is the broker's "brain": it enforces that produce/fetch go to the partition
 * leader, drives {@code acks=all} commit waits, and builds cluster metadata.
 *
 * <p>Within the {@code broker} package the unqualified {@code Log} is the partition commit log; the
 * logging utility is referenced as {@code com.minikafka.common.Log}.
 */
public final class RequestHandler implements RequestDispatcher {
    private static final com.minikafka.common.Log LOG = com.minikafka.common.Log.of(RequestHandler.class);

    /** Marks a CREATE_TOPICS forwarded broker-to-broker, so it is applied locally but not re-forwarded. */
    private static final String INTERNAL_CLIENT_ID = "__mk_internal";

    private final int brokerId;
    private final LogManagerRef logs;
    private final ReplicaManager replicaManager;
    private final Cluster cluster;
    private final OffsetManager offsetManager;
    private final Metrics metrics;
    private final BrokerConfig config;
    private final GroupCoordinator groupCoordinator;

    /** The local Raft node, set once the broker wires up its controller quorum (null if raft disabled). */
    private volatile RaftNode raftNode;

    /** Narrow view of {@link com.minikafka.storage.LogManager} to avoid another Log name clash in imports. */
    public interface LogManagerRef {
        Log getOrCreateLog(TopicPartition tp);
    }

    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public RequestHandler(int brokerId, LogManagerRef logs, ReplicaManager replicaManager,
                          Cluster cluster, OffsetManager offsetManager, Metrics metrics, BrokerConfig config,
                          GroupCoordinator groupCoordinator) {
        this.brokerId = brokerId;
        this.logs = logs;
        this.replicaManager = replicaManager;
        this.cluster = cluster;
        this.offsetManager = offsetManager;
        this.metrics = metrics;
        this.config = config;
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public byte[] dispatch(RequestHeader header, ByteBuffer body) {
        ApiKeys api = ApiKeys.forId(header.apiKey);
        if (api == null) {
            LOG.warn("unknown api key {}", header.apiKey);
            return new byte[0];
        }
        switch (api) {
            case API_VERSIONS:
                return handleApiVersions();
            case METADATA:
                return handleMetadata(body);
            case CREATE_TOPICS:
                return handleCreateTopics(header, body);
            case PRODUCE:
                return handleProduce(body);
            case FETCH:
                return handleFetch(body);
            case LIST_OFFSETS:
                return handleListOffsets(body);
            case OFFSET_COMMIT:
                return handleOffsetCommit(body);
            case OFFSET_FETCH:
                return handleOffsetFetch(body);
            case JOIN_GROUP:
                return serialize(groupCoordinator.joinGroup(JoinGroupRequest.parse(body))::writeTo);
            case SYNC_GROUP:
                return serialize(groupCoordinator.syncGroup(SyncGroupRequest.parse(body))::writeTo);
            case HEARTBEAT:
                return serialize(groupCoordinator.heartbeat(HeartbeatRequest.parse(body))::writeTo);
            case LEAVE_GROUP:
                return serialize(groupCoordinator.leaveGroup(LeaveGroupRequest.parse(body))::writeTo);
            case RAFT_REQUEST_VOTE:
                return handleRaftRequestVote(body);
            case RAFT_APPEND_ENTRIES:
                return handleRaftAppendEntries(body);
            default:
                return new byte[0];
        }
    }

    private static byte[] serialize(java.util.function.Consumer<ByteWriter> writer) {
        ByteWriter out = new ByteWriter();
        writer.accept(out);
        return out.toByteArray();
    }

    private byte[] handleRaftRequestVote(ByteBuffer body) {
        RaftNode node = raftNode;
        if (node == null) {
            return new byte[0];
        }
        RequestVoteRequest req = RequestVoteRequest.parse(body);
        try {
            RequestVoteResponse resp = node.receiveRequestVote(req).get(2, TimeUnit.SECONDS);
            ByteWriter out = new ByteWriter();
            resp.writeTo(out);
            return out.toByteArray();
        } catch (Exception e) {
            return new byte[0]; // sender treats an unparsable reply as a failed RPC and retries
        }
    }

    private byte[] handleRaftAppendEntries(ByteBuffer body) {
        RaftNode node = raftNode;
        if (node == null) {
            return new byte[0];
        }
        AppendEntriesRequest req = AppendEntriesRequest.parse(body);
        try {
            AppendEntriesResponse resp = node.receiveAppendEntries(req).get(2, TimeUnit.SECONDS);
            ByteWriter out = new ByteWriter();
            resp.writeTo(out);
            return out.toByteArray();
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private byte[] handleApiVersions() {
        ByteWriter out = new ByteWriter();
        ApiVersionsResponse.supported().writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleMetadata(ByteBuffer body) {
        MetadataRequest req = MetadataRequest.parse(body);
        List<String> requested = req.topics == null ? cluster.topicNames() : req.topics;

        List<MetadataResponse.Broker> brokers = new ArrayList<>();
        for (Node n : cluster.nodes()) {
            brokers.add(new MetadataResponse.Broker(n.id(), n.host(), n.port()));
        }

        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (String name : requested) {
            TopicConfig tc = cluster.getTopic(name);
            if (tc == null) {
                topics.add(new MetadataResponse.TopicMetadata(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION, name, new ArrayList<>()));
                continue;
            }
            List<MetadataResponse.PartitionMetadata> parts = new ArrayList<>();
            for (int p = 0; p < tc.numPartitions; p++) {
                TopicPartition tp = new TopicPartition(name, p);
                int[] replicas = cluster.replicas(tp);
                int leader = cluster.leader(tp);
                int[] isr = replicaManager.isr(tp);
                parts.add(new MetadataResponse.PartitionMetadata(
                        Errors.NONE, p, leader,
                        replicas == null ? new int[0] : replicas, isr));
            }
            topics.add(new MetadataResponse.TopicMetadata(Errors.NONE, name, parts));
        }

        ByteWriter out = new ByteWriter();
        new MetadataResponse(brokers, topics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleProduce(ByteBuffer body) {
        ProduceRequest req = ProduceRequest.parse(body);
        List<ProduceResponse.Topic> respTopics = new ArrayList<>();
        for (ProduceRequest.Topic t : req.topics) {
            List<ProduceResponse.Partition> respParts = new ArrayList<>();
            for (ProduceRequest.Partition p : t.partitions) {
                TopicPartition tp = new TopicPartition(t.name, p.index);
                short err = Errors.NONE;
                long baseOffset = -1;

                int[] replicas = cluster.replicas(tp);
                if (replicas == null) {
                    err = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                } else if (cluster.leader(tp) != brokerId) {
                    err = Errors.NOT_LEADER_FOR_PARTITION;
                } else {
                    Log log = logs.getOrCreateLog(tp);
                    long base = -1;
                    int count = 0;
                    long bytes = 0;
                    for (ProduceRequest.Record r : p.records) {
                        long off = log.append(r.key, r.value);
                        if (base < 0) {
                            base = off;
                        }
                        count++;
                        bytes += (r.key == null ? 0 : r.key.length)
                                + (r.value == null ? 0 : r.value.length);
                    }
                    baseOffset = count == 0 ? log.logEndOffset() : base;
                    metrics.onProduce(count, bytes);
                    if (req.acks == -1 && count > 0) {
                        long target = base + count; // exclusive: all these offsets must be committed
                        int timeout = req.timeoutMs > 0 ? req.timeoutMs : config.produceTimeoutMs();
                        if (!replicaManager.awaitHighWatermark(tp, target, timeout)) {
                            err = Errors.REQUEST_TIMED_OUT;
                        }
                    }
                }
                respParts.add(new ProduceResponse.Partition(p.index, err, baseOffset));
            }
            respTopics.add(new ProduceResponse.Topic(t.name, respParts));
        }

        ByteWriter out = new ByteWriter();
        new ProduceResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleFetch(ByteBuffer body) {
        FetchRequest req = FetchRequest.parse(body);
        boolean fromFollower = req.isFromFollower();
        List<FetchResponse.Topic> respTopics = new ArrayList<>();
        for (FetchRequest.Topic t : req.topics) {
            List<FetchResponse.Partition> respParts = new ArrayList<>();
            for (FetchRequest.Partition p : t.partitions) {
                TopicPartition tp = new TopicPartition(t.name, p.index);
                short err = Errors.NONE;
                long hw = 0;
                long logStart = 0;
                List<FetchResponse.Record> records = new ArrayList<>();

                int[] replicas = cluster.replicas(tp);
                if (replicas == null) {
                    err = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                } else if (cluster.leader(tp) != brokerId) {
                    err = Errors.NOT_LEADER_FOR_PARTITION;
                } else {
                    if (fromFollower) {
                        replicaManager.recordFollowerFetch(tp, req.replicaId, p.fetchOffset);
                    }
                    Log log = logs.getOrCreateLog(tp);
                    LogReadResult res = log.read(p.fetchOffset, p.maxBytes, fromFollower);
                    err = res.errorCode;
                    hw = res.highWatermark;
                    logStart = res.logStartOffset;
                    for (Record r : res.records) {
                        records.add(new FetchResponse.Record(r.offset, r.timestamp, r.key, r.value));
                    }
                    if (!fromFollower && !records.isEmpty()) {
                        metrics.onFetch(records.size());
                    }
                }
                respParts.add(new FetchResponse.Partition(p.index, err, hw, logStart, records));
            }
            respTopics.add(new FetchResponse.Topic(t.name, respParts));
        }

        ByteWriter out = new ByteWriter();
        new FetchResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleListOffsets(ByteBuffer body) {
        ListOffsetsRequest req = ListOffsetsRequest.parse(body);
        List<ListOffsetsResponse.Topic> respTopics = new ArrayList<>();
        for (ListOffsetsRequest.Topic t : req.topics) {
            List<ListOffsetsResponse.Partition> respParts = new ArrayList<>();
            for (ListOffsetsRequest.Partition p : t.partitions) {
                TopicPartition tp = new TopicPartition(t.name, p.index);
                short err = Errors.NONE;
                long offset = -1;

                int[] replicas = cluster.replicas(tp);
                if (replicas == null) {
                    err = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                } else if (cluster.leader(tp) != brokerId) {
                    err = Errors.NOT_LEADER_FOR_PARTITION;
                } else {
                    Log log = logs.getOrCreateLog(tp);
                    offset = p.timestamp == ListOffsetsRequest.EARLIEST
                            ? log.logStartOffset()
                            : log.highWatermark();
                }
                respParts.add(new ListOffsetsResponse.Partition(p.index, err, p.timestamp, offset));
            }
            respTopics.add(new ListOffsetsResponse.Topic(t.name, respParts));
        }

        ByteWriter out = new ByteWriter();
        new ListOffsetsResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleOffsetCommit(ByteBuffer body) {
        OffsetCommitRequest req = OffsetCommitRequest.parse(body);
        List<OffsetCommitResponse.Topic> respTopics = new ArrayList<>();
        for (OffsetCommitRequest.Topic t : req.topics) {
            List<OffsetCommitResponse.Partition> respParts = new ArrayList<>();
            for (OffsetCommitRequest.Partition p : t.partitions) {
                TopicPartition tp = new TopicPartition(t.name, p.index);
                offsetManager.commit(req.groupId, tp, p.committedOffset, p.metadata);
                respParts.add(new OffsetCommitResponse.Partition(p.index, Errors.NONE));
            }
            respTopics.add(new OffsetCommitResponse.Topic(t.name, respParts));
        }

        ByteWriter out = new ByteWriter();
        new OffsetCommitResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleOffsetFetch(ByteBuffer body) {
        OffsetFetchRequest req = OffsetFetchRequest.parse(body);
        List<OffsetFetchResponse.Topic> respTopics = new ArrayList<>();
        for (OffsetFetchRequest.Topic t : req.topics) {
            List<OffsetFetchResponse.Partition> respParts = new ArrayList<>();
            for (int index : t.partitions) {
                TopicPartition tp = new TopicPartition(t.name, index);
                OffsetManager.OffsetAndMetadata om = offsetManager.fetch(req.groupId, tp);
                long committed = om == null ? -1 : om.offset;
                String metadata = om == null ? null : om.metadata;
                respParts.add(new OffsetFetchResponse.Partition(index, committed, metadata, Errors.NONE));
            }
            respTopics.add(new OffsetFetchResponse.Topic(t.name, respParts));
        }

        ByteWriter out = new ByteWriter();
        new OffsetFetchResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    private byte[] handleCreateTopics(RequestHeader header, ByteBuffer body) {
        CreateTopicsRequest req = CreateTopicsRequest.parse(body);
        boolean internal = INTERNAL_CLIENT_ID.equals(header.clientId);

        List<CreateTopicsResponse.Topic> respTopics = new ArrayList<>();
        List<CreateTopicsRequest.Topic> created = new ArrayList<>();
        for (CreateTopicsRequest.Topic t : req.topics) {
            short err;
            if (cluster.hasTopic(t.name)) {
                err = Errors.TOPIC_ALREADY_EXISTS;
            } else {
                int parts = t.numPartitions <= 0 ? 1 : t.numPartitions;
                int rf = t.replicationFactor <= 0 ? 1 : t.replicationFactor;
                cluster.addOrUpdateTopic(new TopicConfig(t.name, parts, rf));
                replicaManager.addPartitionsForTopic(t.name);
                created.add(new CreateTopicsRequest.Topic(t.name, parts, (short) rf));
                err = Errors.NONE;
                LOG.info("created topic {} (partitions={}, rf={})", t.name, parts, rf);
            }
            respTopics.add(new CreateTopicsResponse.Topic(t.name, err));
        }

        if (!internal && !created.isEmpty()) {
            propagateCreateTopics(new CreateTopicsRequest(created, req.timeoutMs));
        }

        ByteWriter out = new ByteWriter();
        new CreateTopicsResponse(respTopics).writeTo(out);
        return out.toByteArray();
    }

    /** Best-effort broadcast of a topic creation to peer brokers so their cluster view stays in sync. */
    private void propagateCreateTopics(CreateTopicsRequest req) {
        ByteWriter body = new ByteWriter();
        req.writeTo(body);
        byte[] bytes = body.toByteArray();
        for (Node node : cluster.nodes()) {
            if (node.id() == brokerId) {
                continue;
            }
            try (BrokerConnection conn = new BrokerConnection(node.host(), node.port(), 3000)) {
                conn.send(ApiKeys.CREATE_TOPICS.id, (short) 0, INTERNAL_CLIENT_ID, bytes);
            } catch (Exception e) {
                LOG.warn("failed to propagate topic creation to {}: {}", node, e.toString());
            }
        }
    }
}