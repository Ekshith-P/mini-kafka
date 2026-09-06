package com.minikafka.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The Raft state that MUST survive a crash: the current term, who this node voted for in that term,
 * and the log. Raft's safety proof depends on these being durable before a node replies to an RPC.
 */
public final class PersistentState {

    public final long currentTerm;
    public final int votedFor; // -1 = none
    public final List<LogEntry> log;

    public PersistentState(
            long currentTerm,
            int votedFor,
            List<LogEntry> log) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
    }

    public static PersistentState empty() {
        return new PersistentState(
                0,
                -1,
                new ArrayList<>()
        );
    }
}