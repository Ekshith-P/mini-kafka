package com.minikafka.raft;

import java.util.function.BiConsumer;

/**
 * Sends Raft RPCs to peers. Deliberately asynchronous: the caller passes a callback that fires with
 * the peer's reply (or {@code null} if the RPC failed/timed out), which the {@link RaftNode} then
 * hands back to its single-threaded executor. This keeps all Raft state mutation on one thread while
 * network I/O happens elsewhere.
 *
 * <p>Two implementations exist: an in-memory one for tests (a simulated network) and a socket-based
 * one for a real cluster.
 */
public interface RaftTransport {
    void requestVote(int toNodeId, RequestVoteRequest request,
                    BiConsumer<Integer, RequestVoteResponse> onReply);

    void appendEntries(int toNodeId, AppendEntriesRequest request,
                       BiConsumer<Integer, AppendEntriesResponse> onReply);
}