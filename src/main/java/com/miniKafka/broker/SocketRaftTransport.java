package com.minikafka.broker;

import com.minikafka.client.BrokerConnection;
import com.minikafka.common.ApiKeys;
import com.minikafka.common.Node;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.raft.AppendEntriesRequest;
import com.minikafka.raft.AppendEntriesResponse;
import com.minikafka.raft.RaftTransport;
import com.minikafka.raft.RequestVoteRequest;
import com.minikafka.raft.RequestVoteResponse;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * {@link RaftTransport} over TCP, using the broker's ordinary request framing with dedicated
 * {@code RAFT_*} api keys. RPCs run on a thread pool so the Raft thread never blocks, and at most one
 * RPC is in flight per peer (a fresh heartbeat is dropped if the previous one hasn't returned) so a
 * dead or slow peer can never build an unbounded backlog.
 */
public final class SocketRaftTransport implements RaftTransport {
    private final int selfId;
    private final Map<Integer, Node> peers;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private final ExecutorService pool;
    private final Map<Integer, BrokerConnection> connections = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicBoolean> inFlight = new ConcurrentHashMap<>();

    public SocketRaftTransport(int selfId, Map<Integer, Node> peers, int connectTimeoutMs, int readTimeoutMs) {
        this.selfId = selfId;
        this.peers = new ConcurrentHashMap<>(peers);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "mk-raft-tx-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void requestVote(int to, RequestVoteRequest request,
                            BiConsumer<Integer, RequestVoteResponse> onReply) {
        send(to, ApiKeys.RAFT_REQUEST_VOTE.id, w -> request.writeTo(w),
                RequestVoteResponse::parse, onReply);
    }

    @Override
    public void appendEntries(int to, AppendEntriesRequest request,
                              BiConsumer<Integer, AppendEntriesResponse> onReply) {
        send(to, ApiKeys.RAFT_APPEND_ENTRIES.id, w -> request.writeTo(w),
                AppendEntriesResponse::parse, onReply);
    }

    private <R> void send(int to, short apiKey, java.util.function.Consumer<ByteWriter> writer,
                          java.util.function.Function<ByteBuffer, R> parser, BiConsumer<Integer, R> onReply) {
        if (!gate(to)) {
            return; // an RPC to this peer is already outstanding; drop this one
        }
        try {
            pool.execute(() -> {
                R response = null;
                try {
                    BrokerConnection conn = connection(to);
                    ByteWriter w = new ByteWriter();
                    writer.accept(w);
                    ByteBuffer buf = conn.send(apiKey, (short) 0, "raft-" + selfId, w.toByteArray());
                    response = parser.apply(buf);
                } catch (Exception e) {
                    dropConnection(to);
                } finally {
                    ungate(to);
                }
                onReply.accept(to, response);
            });
        } catch (RejectedExecutionException e) {
            ungate(to);
            onReply.accept(to, null);
        }
    }

    private BrokerConnection connection(int to) throws Exception {
        BrokerConnection conn = connections.get(to);
        if (conn == null) {
            Node node = peers.get(to);
            conn = new BrokerConnection(node.host(), node.port(), connectTimeoutMs, readTimeoutMs);
            connections.put(to, conn);
        }
        return conn;
    }

    private void dropConnection(int to) {
        BrokerConnection conn = connections.remove(to);
        if (conn != null) {
            conn.close();
        }
    }

    private boolean gate(int to) {
        return inFlight.computeIfAbsent(to, k -> new AtomicBoolean()).compareAndSet(false, true);
    }

    private void ungate(int to) {
        inFlight.get(to).set(false);
    }

    public void stop() {
        pool.shutdownNow();
        for (BrokerConnection conn : connections.values()) {
            conn.close();
        }
        connections.clear();
    }
}