package com.minikafka.broker;

import com.minikafka.common.Errors;
import com.minikafka.common.Log;
import com.minikafka.common.TopicPartition;
import com.minikafka.protocol.messages.HeartbeatRequest;
import com.minikafka.protocol.messages.HeartbeatResponse;
import com.minikafka.protocol.messages.JoinGroupRequest;
import com.minikafka.protocol.messages.JoinGroupResponse;
import com.minikafka.protocol.messages.LeaveGroupRequest;
import com.minikafka.protocol.messages.LeaveGroupResponse;
import com.minikafka.protocol.messages.SyncGroupRequest;
import com.minikafka.protocol.messages.SyncGroupResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages consumer groups for the broker that is a group's coordinator: membership, generations,
 * partition assignment, and rebalancing. Modeled on Kafka's group protocol:
 *
 * <ol>
 *   <li><b>JoinGroup</b> - the reply is held open until <em>all</em> current members have rejoined
 *       into the same generation (the "rebalance barrier"), with a timeout fallback. This is what
 *       stops members from ping-ponging generations.</li>
 *   <li><b>SyncGroup</b> - each member fetches its assigned partitions.</li>
 *   <li><b>Heartbeat</b> - members prove liveness; if the generation moved or the member timed out,
 *       they're told to rejoin.</li>
 * </ol>
 *
 * <p>Assignment is computed <b>server-side</b> (round-robin partitions over sorted member ids). Real
 * Kafka delegates this to a client-side assignor shipped through Sync/JoinGroup; server-side is
 * simpler and demonstrates the same coordinator + rebalance mechanics.
 */
public final class GroupCoordinator {
    private static final Log LOG = Log.of(GroupCoordinator.class);

    private enum State { PREPARING_REBALANCE, STABLE }

    private static final class Member {
        final String memberId;
        List<String> topics;
        int sessionTimeoutMs;
        long lastHeartbeatMs;
        List<TopicPartition> assignment = Collections.emptyList();

        Member(String memberId, List<String> topics, int sessionTimeoutMs) {
            this.memberId = memberId;
            this.topics = topics;
            this.sessionTimeoutMs = sessionTimeoutMs;
        }
    }

    private static final class Group {
        final String groupId;
        final Object lock = new Object();
        final Map<String, Member> members = new LinkedHashMap<>();
        final Map<String, CompletableFuture<Integer>> pendingJoins = new LinkedHashMap<>();
        int generationId;
        State state = State.STABLE;
        long rebalanceStartMs;

        Group(String groupId) {
            this.groupId = groupId;
        }
    }

    private final Cluster cluster;
    private final long rebalanceTimeoutMs;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Group> groups = new ConcurrentHashMap<>();

    public GroupCoordinator(Cluster cluster, long rebalanceTimeoutMs) {
        this.cluster = cluster;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mk-group-coordinator");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::maintenance, 300, 300, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    // ----- JoinGroup (blocks on the rebalance barrier) ------------------------------------

    public JoinGroupResponse joinGroup(JoinGroupRequest req) {
        Group g = groups.computeIfAbsent(req.groupId, Group::new);
        String memberId;
        CompletableFuture<Integer> future;
        synchronized (g.lock) {
            memberId = (req.memberId == null || req.memberId.isEmpty())
                    ? "consumer-" + UUID.randomUUID()
                    : req.memberId;
            Member member = g.members.get(memberId);
            if (member == null) {
                member = new Member(memberId, req.topics, req.sessionTimeoutMs);
                g.members.put(memberId, member);
            } else {
                member.topics = req.topics;
                member.sessionTimeoutMs = req.sessionTimeoutMs;
            }
            member.lastHeartbeatMs = System.currentTimeMillis();

            if (g.state == State.STABLE) {
                beginRebalance(g);
            }
            future = new CompletableFuture<>();
            g.pendingJoins.put(memberId, future);
            maybeFinalizeEarly(g);
        }
        try {
            int generation = future.get(rebalanceTimeoutMs + 5000, TimeUnit.MILLISECONDS);
            return new JoinGroupResponse(Errors.NONE, generation, memberId);
        } catch (Exception e) {
            return new JoinGroupResponse(Errors.REBALANCE_IN_PROGRESS, -1, memberId);
        }
    }

    private void beginRebalance(Group g) {
        g.state = State.PREPARING_REBALANCE;
        g.rebalanceStartMs = System.currentTimeMillis();
    }

    /** Finalizes as soon as every current member has a pending join in this rebalance. */
    private void maybeFinalizeEarly(Group g) {
        if (g.state == State.PREPARING_REBALANCE && g.pendingJoins.keySet().containsAll(g.members.keySet())) {
            finalizeRebalance(g);
        }
    }

    private void finalizeRebalance(Group g) {
        g.generationId++;
        computeAssignment(g);
        g.state = State.STABLE;
        for (CompletableFuture<Integer> f : g.pendingJoins.values()) {
            f.complete(g.generationId);
        }
        g.pendingJoins.clear();
        LOG.info("group {} rebalanced to generation {} with {} member(s)",
                g.groupId, g.generationId, g.members.size());
    }

    /** Round-robins every subscribed partition across the sorted member ids. */
    private void computeAssignment(Group g) {
        TreeSet<String> topics = new TreeSet<>();
        for (Member m : g.members.values()) {
            topics.addAll(m.topics);
        }
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            TopicConfig tc = cluster.getTopic(topic);
            if (tc == null) {
                continue;
            }
            for (int p = 0; p < tc.numPartitions; p++) {
                allPartitions.add(new TopicPartition(topic, p));
            }
        }
        List<String> memberIds = new ArrayList<>(g.members.keySet());
        Collections.sort(memberIds);
        for (Member m : g.members.values()) {
            m.assignment = new ArrayList<>();
        }
        if (!memberIds.isEmpty()) {
            for (int i = 0; i < allPartitions.size(); i++) {
                g.members.get(memberIds.get(i % memberIds.size())).assignment.add(allPartitions.get(i));
            }
        }
    }

    // ----- SyncGroup / Heartbeat / LeaveGroup ----------------------------------------------

    public SyncGroupResponse syncGroup(SyncGroupRequest req) {
        Group g = groups.get(req.groupId);
        if (g == null) {
            return new SyncGroupResponse(Errors.UNKNOWN_MEMBER_ID, Collections.emptyList());
        }
        synchronized (g.lock) {
            Member member = g.members.get(req.memberId);
            if (member == null) {
                return new SyncGroupResponse(Errors.UNKNOWN_MEMBER_ID, Collections.emptyList());
            }
            if (g.state != State.STABLE || req.generationId != g.generationId) {
                return new SyncGroupResponse(Errors.REBALANCE_IN_PROGRESS, Collections.emptyList());
            }
            member.lastHeartbeatMs = System.currentTimeMillis();

            Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
            for (TopicPartition tp : member.assignment) {
                byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
            }
            List<SyncGroupResponse.Assignment> assignments = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
                assignments.add(new SyncGroupResponse.Assignment(e.getKey(), toIntArray(e.getValue())));
            }
            return new SyncGroupResponse(Errors.NONE, assignments);
        }
    }

    public HeartbeatResponse heartbeat(HeartbeatRequest req) {
        Group g = groups.get(req.groupId);
        if (g == null) {
            return new HeartbeatResponse(Errors.UNKNOWN_MEMBER_ID);
        }
        synchronized (g.lock) {
            Member member = g.members.get(req.memberId);
            if (member == null) {
                return new HeartbeatResponse(Errors.UNKNOWN_MEMBER_ID);
            }
            if (g.state == State.PREPARING_REBALANCE) {
                return new HeartbeatResponse(Errors.REBALANCE_IN_PROGRESS);
            }
            if (req.generationId != g.generationId) {
                return new HeartbeatResponse(Errors.ILLEGAL_GENERATION);
            }
            member.lastHeartbeatMs = System.currentTimeMillis();
            return new HeartbeatResponse(Errors.NONE);
        }
    }

    public LeaveGroupResponse leaveGroup(LeaveGroupRequest req) {
        Group g = groups.get(req.groupId);
        if (g == null) {
            return new LeaveGroupResponse(Errors.NONE);
        }
        synchronized (g.lock) {
            if (g.members.remove(req.memberId) != null) {
                g.pendingJoins.remove(req.memberId);
                LOG.info("group {} member {} left", g.groupId, req.memberId);
                if (g.state == State.STABLE) {
                    beginRebalance(g);
                }
                maybeFinalizeEarly(g);
            }
            return new LeaveGroupResponse(Errors.NONE);
        }
    }

    private void maintenance() {
        long now = System.currentTimeMillis();
        for (Group g : groups.values()) {
            synchronized (g.lock) {
                boolean removed = false;
                Iterator<Map.Entry<String, Member>> it = g.members.entrySet().iterator();
                while (it.hasNext()) {
                    Member m = it.next().getValue();
                    if (now - m.lastHeartbeatMs > m.sessionTimeoutMs) {
                        it.remove();
                        g.pendingJoins.remove(m.memberId);
                        removed = true;
                        LOG.info("group {} evicted member {} (session timeout)", g.groupId, m.memberId);
                    }
                }
                if (removed && g.state == State.STABLE) {
                    beginRebalance(g);
                }
                if (g.state == State.PREPARING_REBALANCE) {
                    maybeFinalizeEarly(g);
                    // Fallback: if some members never rejoined, finalize without them.
                    if (g.state == State.PREPARING_REBALANCE && now - g.rebalanceStartMs > rebalanceTimeoutMs) {
                        if (!g.pendingJoins.isEmpty()) {
                            g.members.keySet().removeIf(id -> !g.pendingJoins.containsKey(id));
                        }
                        finalizeRebalance(g);
                    }
                }
            }
        }
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] out = new int[list.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = list.get(i);
        }
        return out;
    }
}