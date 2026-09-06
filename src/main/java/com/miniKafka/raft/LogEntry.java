package com.minikafka.raft;

/**
 * One entry in the Raft replicated log. Entries are 1-indexed; index 0 is a virtual empty entry
 * (term 0) used only for the "previous entry" of the very first real entry.
 *
 * @param index 1-based position in the log
 * @param term the leader's term when this entry was created (used for the log-matching safety check)
 * @param command opaque bytes handed to the {@link StateMachine} once the entry is committed
 */
public final class LogEntry {

    public final long index;
    public final long term;
    public final byte[] command;

    public LogEntry(long index, long term, byte[] command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    @Override
    public String toString() {
        return "LogEntry(index=" + index
                + ", term=" + term
                + ", cmd=" + command.length + "B)";
    }
}