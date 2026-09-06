package com.minikafka.raft;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;

/** Raft RPC: a candidate asks a peer for its vote in {@code term}. */
public final class RequestVoteRequest {

    public final long term;
    public final int candidateId;
    public final long lastLogIndex;
    public final long lastLogTerm;

    public RequestVoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public void writeTo(ByteWriter out) {
        out.putLong(term);
        out.putInt(candidateId);
        out.putLong(lastLogIndex);
        out.putLong(lastLogTerm);
    }

    public static RequestVoteRequest parse(ByteBuffer buf) {
        return new RequestVoteRequest(
                buf.getLong(),
                buf.getInt(),
                buf.getLong(),
                buf.getLong()
        );
    }
}