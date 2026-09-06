package com.minikafka.raft;

import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft RPC: the leader replicates log entries to a follower (an empty {@code entries} list is a
 * heartbeat). {@code prevLogIndex}/{@code prevLogTerm} are the entry immediately before the new ones;
 * the follower only accepts this prefix if its log matches there — the log-matching property that keeps all logs
 * consistent.
 */
public final class AppendEntriesRequest {

    public final long term;
    public final int leaderId;
    public final long prevLogIndex;
    public final long prevLogTerm;
    public final List<LogEntry> entries;
    public final long leaderCommit;

    public AppendEntriesRequest(
            long term,
            int leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public void writeTo(ByteWriter out) {
        out.putLong(term);
        out.putLong(leaderId);
        out.putLong(prevLogIndex);
        out.putLong(prevLogTerm);
        out.putLong(leaderCommit);
        out.putInt(entries.size());

        for (LogEntry e : entries) {
            out.putLong(e.index);
            out.putLong(e.term);
            out.putBytes(e.command);
        }
    }

    public static AppendEntriesRequest parse(ByteBuffer buf) {
        long term = buf.getLong();
        int leaderId = buf.getInt();
        long prevLogIndex = buf.getLong();
        long prevLogTerm = buf.getLong();
        long leaderCommit = buf.getLong();

        int count = buf.getInt();
        List<LogEntry> entries =
                new ArrayList<>(Math.max(0, count));

        for (int i = 0; i < count; i++) {
            long index = buf.getLong();
            long entryTerm = buf.getLong();
            byte[] command = Protocol.getBytes(buf);
            entries.add(new LogEntry(index, entryTerm, command));
        }

        return new AppendEntriesRequest(
                term,
                leaderId,
                prevLogIndex,
                prevLogTerm,
                entries,
                leaderCommit);
    }
}