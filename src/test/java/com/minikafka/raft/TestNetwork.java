package com.minikafka.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * A simulated network for testing Raft without sockets. It wires nodes together with in-memory
 * {@link RaftTransport}s and can {@link #disconnect} / {@link #reconnect} nodes to model partitions
 * and crashes. RPCs are delivered by calling the target node's {@code receive*} methods (which hop
 * onto that node's own Raft thread), so the actor model is preserved end to end.
 */
final class TestNetwork {
    private final ConcurrentHashMap<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Set<Integer> disconnected = ConcurrentHashMap.newKeySet();

    RaftTransport transportFor(int selfId) {
        return new RaftTransport() {
            @Override
            public void requestVote(int to, RequestVoteRequest req,
                                    BiConsumer<Integer, RequestVoteResponse> onReply) {
                RaftNode target = connected(selfId, to) ? nodes.get(to) : null;
                if (target == null) {
                    onReply.accept(to, null);
                    return;
                }
                target.receiveRequestVote(req).whenComplete((resp, ex) ->
                        onReply.accept(to, ex != null || !connected(selfId, to) ? null : resp));
            }

            @Override
            public void appendEntries(int to, AppendEntriesRequest req,
                                      BiConsumer<Integer, AppendEntriesResponse> onReply) {
                RaftNode target = connected(selfId, to) ? nodes.get(to) : null;
                if (target == null) {
                    onReply.accept(to, null);
                    return;
                }
                target.receiveAppendEntries(req).whenComplete((resp, ex) ->
                        onReply.accept(to, ex != null || !connected(selfId, to) ? null : resp));
            }
        };
    }

    private boolean connected(int a, int b) {
        return !disconnected.contains(a) && !disconnected.contains(b);
    }

    void register(RaftNode node) {
        nodes.put(node.id(), node);
    }

    void disconnect(int id) {
        disconnected.add(id);
    }

    void reconnect(int id) {
        disconnected.remove(id);
    }

    void startAll() {
        for (RaftNode n : nodes.values()) {
            n.start();
        }
    }

    void stopAll() {
        for (RaftNode n : nodes.values()) {
            n.stop();
        }
    }

    List<RaftNode> nodes() {
        return new ArrayList<>(nodes.values());
    }

    long connectedLeaderCount() {
        return nodes.values().stream()
                .filter(n -> !disconnected.contains(n.id()))
                .filter(RaftNode::isLeader)
                .count();
    }

    RaftNode awaitLeader(long timeoutMs) throws InterruptedException {
        return awaitLeaderExcluding(Set.of(), timeoutMs);
    }

    RaftNode awaitLeaderExcluding(Set<Integer> excluded, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : nodes.values()) {
                if (excluded.contains(n.id()) || disconnected.contains(n.id())) {
                    continue;
                }
                if (n.isLeader()) {
                    return n;
                }
            }
            Thread.sleep(20);
        }
        return null;
    }
}