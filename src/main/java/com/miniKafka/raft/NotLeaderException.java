package com.minikafka.raft;

/**
 * Thrown when a command is proposed to a node that is not the current leader. Carries a hint to the
 * last-known leader so the caller can redirect (or -1 if unknown).
 */
public final class NotLeaderException extends RuntimeException {

    public final int leaderHint;

    public NotLeaderException(int leaderHint) {
        super("not the leader; try broker " + leaderHint);
        this.leaderHint = leaderHint;
    }
}