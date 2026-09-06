package com.minikafka.raft;

/**
 * The application state built by replaying committed log entries in order. Raft guarantees every
 * server applies the same entries in the same order, so every server's state machine ends up
 * identical. In this project the state machine is the cluster metadata (topics, partition leaders,
 * broker liveness).
 */
public interface StateMachine {

    /**
     * Applies one committed command. Called on the Raft thread, strictly in increasing index order,
     * exactly once per entry.
     */
    void apply(long index, byte[] command);
}