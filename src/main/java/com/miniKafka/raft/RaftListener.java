package com.minikafka.raft;

/** Notified when this node's role changes, so higher layers (the controller) can react. */
public interface RaftListener {

    /** Called on the Raft thread whenever the role, term, or known leader changes. */
    default void onRoleChange(RaftRole role, long term, int leaderId) {
    }
}