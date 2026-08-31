package com.minikafka.replication;

import com.minikafka.broker.BrokerConfig;
import com.minikafka.broker.Cluster;
import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;
import com.minikafka.storage.Log;
import com.minikafka.storage.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates replication from this broker's perspective, and - new in the KRaft stage -
 * continuously <b>reconciles</b> each partition's role against {@link Cluster#leader}, so leadership
 * can change at runtime after a controller failover.
 *
 * <p><b>As a leader</b> it tracks each follower's replicated offset and last-fetch time, computes the
 * in-sync replica set (ISR), and advances the high-watermark to the smallest log-end offset across
 * the ISR. Only records below the high-watermark are visible to consumers, which is what makes a
 * committed record durable.
 *
 * <p><b>As a follower</b> it runs a {@link ReplicaFetcher} pointed at the current leader. When the
 * controller moves leadership (a broker died), the reconcile loop flips the partition's role: a new
 * leader stops its fetcher and starts accepting produce; a follower repoints its fetcher at the new
 * leader.
 *
 * <p>Note: a broker that was leader, died, and later returns may have a log ahead of the new leader;
 * rejoining it cleanly needs follower log truncation, which is future work. Failover to a surviving
 * replica (the common case) is fully handled.
 */
public final class ReplicaManager {
    private static final com.minikafka.common.Log LOG = com.minikafka.common.Log.of(ReplicaManager.class);

    /** Per-partition leader bookkeeping: each follower's replicated offset and last-fetch time. */
    private static final class LeaderState {
        final Map<Integer, Long> fetchOffset = new ConcurrentHashMap<>();
        final Map<Integer, Long> fetchTimeMs = new ConcurrentHashMap<>();
    }

    private final int brokerId;
    private final LogManager logManager;
    private final Cluster cluster;
    private final BrokerConfig config;

    private final Map<TopicPartition, LeaderState> leaderStates = new ConcurrentHashMap<>();
    private final Map<TopicPartition, ReplicaFetcher> fetchers = new ConcurrentHashMap<>();
    private ScheduledExecutorService maintenance;
    private volatile boolean running;

    public ReplicaManager(int brokerId, LogManager logManager, Cluster cluster, BrokerConfig config) {
        this.brokerId = brokerId;
        this.logManager = logManager;
        this.cluster = cluster;
        this.config = config;
    }

    public void start() {
        running = true;
        reconcileAll();
        long period = Math.max(200, Math.min(500, config.replicaLagTimeMaxMs() / 4));
        maintenance = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mk-replica-maintenance-b" + brokerId);
            t.setDaemon(true);
            return t;
        });
        maintenance.scheduleAtFixedRate(this::maintenanceTick, period, period, TimeUnit.MILLISECONDS);
    }

    private void maintenanceTick() {
        try {
            reconcileAll();
        } catch (RuntimeException e) {
            LOG.warn("reconcile error: {}", e.toString());
        }
        recomputeAllHighWatermarks();
    }

    private void reconcileAll() {
        for (TopicPartition tp : cluster.partitionsHostedBy(brokerId)) {
            reconcilePartition(tp);
        }
    }

    /** Makes this broker's role for {@code tp} match {@link Cluster#leader}. Idempotent. */
    public void reconcilePartition(TopicPartition tp) {
        int[] replicas = cluster.replicas(tp);
        if (replicas == null || replicas.length == 0 || !contains(replicas, brokerId)) {
            return; // this broker doesn't host the partition
        }
        Log log = logManager.getOrCreateLog(tp);
        int leaderId = cluster.leader(tp);
        int rf = replicas.length;

        if (leaderId == brokerId) {
            ReplicaFetcher stale = fetchers.remove(tp);
            if (stale != null) {
                stale.shutdown();
                LOG.info("{}: becoming LEADER (stopped replica fetcher)", tp);
            }
            if (rf <= 1) {
                leaderStates.remove(tp);
                log.setAutoAdvanceHighWatermark(true);
            } else {
                log.setAutoAdvanceHighWatermark(false);
                leaderStates.putIfAbsent(tp, new LeaderState());
            }
        } else {
            leaderStates.remove(tp);
            log.setAutoAdvanceHighWatermark(false);
            Node leaderNode = cluster.nodeById(leaderId);
            if (leaderNode == null) {
                return;
            }
            ReplicaFetcher existing = fetchers.get(tp);
            if (existing == null || existing.leaderId() != leaderId) {
                if (existing != null) {
                    existing.shutdown();
                }
                ReplicaFetcher fetcher = new ReplicaFetcher(tp, leaderNode, log, brokerId,
                        config.replicaFetchIntervalMs(), config.replicaFetchMaxBytes());
                fetchers.put(tp, fetcher);
                fetcher.start();
                LOG.info("{}: following broker {}", tp, leaderId);
            }
        }
    }

    /** Reconciles all partitions of a freshly created topic (used by CREATE_TOPICS at runtime). */
    public void addPartitionsForTopic(String topic) {
        for (TopicPartition tp : cluster.partitionsHostedBy(brokerId)) {
            if (tp.topic().equals(topic)) {
                reconcilePartition(tp);
            }
        }
    }

    private static boolean contains(int[] array, int value) {
        for (int v : array) {
            if (v == value) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records that a follower fetched from {@code requestedOffset} (so it holds everything below it),
     * then recomputes the partition's high-watermark. Called from the leader's fetch handler.
     */
    public void recordFollowerFetch(TopicPartition tp, int followerId, long requestedOffset) {
        LeaderState state = leaderStates.get(tp);
        if (state == null) {
            return; // not a replicated leader for this partition
        }
        state.fetchOffset.put(followerId, requestedOffset);
        state.fetchTimeMs.put(followerId, System.currentTimeMillis());
        recomputeHighWatermark(tp);
    }

    private void recomputeAllHighWatermarks() {
        for (TopicPartition tp : leaderStates.keySet()) {
            try {
                recomputeHighWatermark(tp);
            } catch (RuntimeException e) {
                LOG.warn("HW recompute failed for {}: {}", tp, e.toString());
            }
        }
    }

    private void recomputeHighWatermark(TopicPartition tp) {
        Log log = logManager.getLog(tp);
        LeaderState state = leaderStates.get(tp);
        int[] replicas = cluster.replicas(tp);
        if (log == null || state == null || replicas == null) {
            return;
        }
        long leo = log.logEndOffset();
        long now = System.currentTimeMillis();
        long minLeo = leo; // the leader itself always has the full log
        for (int id : replicas) {
            if (id == brokerId) {
                continue;
            }
            Long lastFetch = state.fetchTimeMs.get(id);
            if (lastFetch != null && now - lastFetch <= config.replicaLagTimeMaxMs()) {
                minLeo = Math.min(minLeo, state.fetchOffset.getOrDefault(id, 0L));
            }
        }
        log.updateHighWatermark(minLeo);
    }

    /** The current in-sync replica set for a partition (leader first). */
    public int[] isr(TopicPartition tp) {
        int[] replicas = cluster.replicas(tp);
        if (replicas == null || replicas.length == 0) {
            return new int[]{brokerId};
        }
        int leaderId = cluster.leader(tp);
        List<Integer> isr = new ArrayList<>();
        isr.add(leaderId);

        LeaderState state = leaderStates.get(tp);
        if (state != null) {
            long now = System.currentTimeMillis();
            for (int id : replicas) {
                if (id == leaderId) {
                    continue;
                }
                Long lastFetch = state.fetchTimeMs.get(id);
                if (lastFetch != null && now - lastFetch <= config.replicaLagTimeMaxMs()) {
                    isr.add(id);
                }
            }
        } else {
            for (int id : replicas) {
                if (id != leaderId) {
                    isr.add(id);
                }
            }
        }
        int[] result = new int[isr.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = isr.get(i);
        }
        return result;
    }

    /**
     * Blocks until the partition's high-watermark reaches {@code targetOffsetExclusive} (all records
     * below it committed to the ISR), or the timeout elapses. Used to satisfy {@code acks=all}.
     */
    public boolean awaitHighWatermark(TopicPartition tp, long targetOffsetExclusive, long timeoutMs) {
        Log log = logManager.getLog(tp);
        if (log == null) {
            return false;
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (log.highWatermark() < targetOffsetExclusive) {
            recomputeHighWatermark(tp);
            if (log.highWatermark() >= targetOffsetExclusive) {
                break;
            }
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    public boolean isLeader(TopicPartition tp) {
        return cluster.isLeader(brokerId, tp);
    }

    public void shutdown() {
        running = false;
        if (maintenance != null) {
            maintenance.shutdownNow();
        }
        for (ReplicaFetcher fetcher : fetchers.values()) {
            fetcher.shutdown();
        }
        LOG.info("replica manager stopped ({} fetcher(s))", fetchers.size());
    }
}