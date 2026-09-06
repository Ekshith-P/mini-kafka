package com.minikafka.raft;

import java.util.ArrayList;

/**
 * Non-durable persistence that just keeps the latest state in memory. Used by tests (and any node
 * that doesn't need to survive a restart). A file-backed implementation is used by real brokers.
 */
public final class InMemoryPersistence implements RaftPersistence {
    private volatile PersistentState state = PersistentState.empty();

    @Override
    public void save(PersistentState state) {
        // Defensive copy of the log so later mutations by the node aren't reflected here.
        this.state = new PersistentState(state.currentTerm, state.votedFor, new ArrayList<>(state.log));
    }

    @Override
    public PersistentState load() {
        return new PersistentState(state.currentTerm, state.votedFor, new ArrayList<>(state.log));
    }
}