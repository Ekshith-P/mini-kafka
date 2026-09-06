package com.minikafka.raft;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;

/**
 * Raft RPC reply. On success, {@code matchIndex} is the highest log index now known to match the
 * leader. On failure, {@code conflictIndex} hints where the logs diverged so the leader can back up
 * {@code nextIndex} quickly instead of one entry at a time.
 */
public final class AppendEntriesResponse {
    public final long term;
    public final boolean success;
    public final long matchIndex;
    public final long conflictIndex;

    public AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = conflictIndex;
    }

    public void writeTo(ByteWriter out) {
        out.putLong(term);
        out.putBool(success);
        out.putLong(matchIndex);
        out.putLong(conflictIndex);
    }

    public static AppendEntriesResponse parse(ByteBuffer buf) {
        return new AppendEntriesResponse(buf.getLong(), buf.get() != 0, buf.getLong(), buf.getLong());
    }
}