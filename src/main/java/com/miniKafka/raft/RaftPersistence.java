package com.minikafka.raft;

/**
 * Durable storage for {@link PersistentState}. {@link RaftNode} calls {@link #save} before replying
 * to any RPC that changed the term, vote, or log, and calls {@link #load} once at startup.
 */
public interface RaftPersistence {

    void save(PersistentState state);

    PersistentState load();
}