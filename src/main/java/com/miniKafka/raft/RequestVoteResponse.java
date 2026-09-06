package com.minikafka.raft;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;

/** Raft RPC reply: whether the peer granted its vote, plus its current term. */
public final class RequestVoteResponse {
    public final long term;
    public final boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public void writeTo(ByteWriter out) {
        out.putLong(term);
        out.putBool(voteGranted);
    }

    public static RequestVoteResponse parse(ByteBuffer buf) {
        return new RequestVoteResponse(buf.getLong(), buf.get() != 0);
    }
}