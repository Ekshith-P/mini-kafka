package com.minikafka.raft;

/**
 * Timing knobs for Raft. The election timeout is randomized in {@code [electionMinMs, electionMaxMs]}
 * on each node so that split votes are rare (nodes almost never time out simultaneously) - the key
 * trick that lets randomized-timeout Raft elect a leader quickly. The heartbeat interval must be
 * comfortably smaller than the election minimum so a live leader keeps followers from timing out.
 */
public final class RaftConfig {
    public final long tickMs;         // how often the node checks its election deadline
    public final long heartbeatMs;    // how often a leader sends AppendEntries
    public final long electionMinMs;
    public final long electionMaxMs;

    public RaftConfig(long tickMs, long heartbeatMs, long electionMinMs, long electionMaxMs) {
        this.tickMs = tickMs;
        this.heartbeatMs = heartbeatMs;
        this.electionMinMs = electionMinMs;
        this.electionMaxMs = electionMaxMs;
    }

    public static RaftConfig defaults() {
        return new RaftConfig(15, 50, 150, 300);
    }
}