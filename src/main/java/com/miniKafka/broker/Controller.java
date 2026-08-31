package com.minikafka.broker;

import com.minikafka.common.Log;
import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;
import com.minikafka.raft.RaftListener;
import com.minikafka.raft.RaftNode;
import com.minikafka.raft.RaftRole;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The cluster controller - the brain that reassigns partition leaders when a broker dies.
 *
 * <p>It runs on whichever broker is the current Raft leader ({@link #onRoleChange} flips it active).
 * On a timer it derives broker liveness from Raft's own heartbeat contact times, and for any
 * partition whose leader has gone silent it proposes a {@code LeaderAndIsr} change (via Raft) that
 * moves leadership to a surviving replica. Because the decision goes through the Raft log, every
 * broker applies the same change and agrees on the new leader - no split brain.
 *
 * <p>Leadership only fails <i>away</i> from dead brokers; it doesn't automatically fail back to a
 * recovered preferred leader (that would be "preferred leader election", left as future work).
 */
public final class Controller implements RaftListener {
    private static final Log LOG = Log.of(Controller.class);

    private final int brokerId;
    private final Cluster cluster;
    private final long livenessTimeoutMs;

    private RaftNode raft;
    private final Set<TopicPartition> failoverInFlight = ConcurrentHashMap.newKeySet();
    private volatile boolean active;
    private volatile long becameControllerAt;
    private ScheduledExecutorService exec;

    public Controller(int brokerId, Cluster cluster, long livenessTimeoutMs) {
        this.brokerId = brokerId;
        this.cluster = cluster;
        this.livenessTimeoutMs = livenessTimeoutMs;
    }

    /** Wired after construction because the Raft node needs this controller as its listener. */
    public void setRaftNode(RaftNode raft) {
        this.raft = raft;
    }

    public void start() {
        long period = Math.max(300, livenessTimeoutMs / 3);
        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mk-controller-b" + brokerId);
            t.setDaemon(true);
            return t;
        });
        exec.scheduleAtFixedRate(this::controlLoop, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (exec != null) {
            exec.shutdownNow();
        }
    }

    @Override
    public void onRoleChange(RaftRole role, long term, int leaderId) {
        boolean nowLeader = role == RaftRole.LEADER;
        if (nowLeader && !active) {
            becameControllerAt = System.currentTimeMillis();
            LOG.info("broker {} became the controller (raft term {})", brokerId, term);
        }
        active = nowLeader;
    }

    private void controlLoop() {
        try {
            if (!active || raft == null || !raft.isLeader()) {
                return;
            }
            // Grace after taking over: give ourselves one liveness window to actually contact the
            // live brokers before we declare anyone dead (a fresh controller starts with no contact
            // history, including for brokers that are perfectly healthy).
            if (System.currentTimeMillis() - becameControllerAt < livenessTimeoutMs) {
                return;
            }
            Set<Integer> alive = computeAliveBrokers();

            for (String topic : cluster.topicNames()) {
                TopicConfig tc = cluster.getTopic(topic);
                if (tc == null) {
                    continue;
                }
                for (int p = 0; p < tc.numPartitions; p++) {
                    reconcilePartitionLeadership(new TopicPartition(topic, p), alive);
                }
            }
        } catch (RuntimeException e) {
            LOG.warn("controller loop error: {}", e.toString());
        }
    }

    private Set<Integer> computeAliveBrokers() {
        Set<Integer> alive = new HashSet<>();
        alive.add(brokerId); // the controller is obviously alive
        long now = System.currentTimeMillis();
        for (Node n : cluster.nodes()) {
            if (n.id() == brokerId) {
                continue;
            }
            long last = raft.lastContactMillis(n.id());
            if (last > 0 && now - last <= livenessTimeoutMs) {
                alive.add(n.id());
            }
        }
        return alive;
    }

    private void reconcilePartitionLeadership(TopicPartition tp, Set<Integer> alive) {
        int[] replicas = cluster.replicas(tp);
        if (replicas == null || replicas.length == 0) {
            return;
        }
        int currentLeader = cluster.leader(tp);
        if (alive.contains(currentLeader) || failoverInFlight.contains(tp)) {
            return; // leader is healthy (or a failover is already in flight)
        }

        int newLeader = -1;
        List<Integer> newIsr = new ArrayList<>();
        for (int replica : replicas) {
            if (alive.contains(replica)) {
                if (newLeader == -1) {
                    newLeader = replica;
                }
                newIsr.add(replica);
            }
        }
        if (newLeader == -1 || newLeader == currentLeader) {
            return; // no surviving replica, or nothing to change
        }

        final int chosenLeader = newLeader;
        LOG.warn("leader broker {} for {} is unreachable; failing over to broker {}",
                currentLeader, tp, chosenLeader);
        int[] isr = newIsr.stream().mapToInt(Integer::intValue).toArray();
        byte[] command = MetadataStateMachine.leaderAndIsr(tp, chosenLeader, isr);
        failoverInFlight.add(tp);
        raft.propose(command).whenComplete((index, ex) -> {
            failoverInFlight.remove(tp);
            if (ex == null) {
                LOG.info("committed failover of {} to broker {} (log index {})", tp, chosenLeader, index);
            }
        });
    }
}