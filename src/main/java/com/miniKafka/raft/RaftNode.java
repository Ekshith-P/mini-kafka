package com.minikafka.raft;

import com.minikafka.common.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A single Raft server implementing leader election and log replication (Raft §5.2–§5.4), enough for
 * a fault-tolerant replicated state machine. Snapshotting and dynamic membership are out of scope.
 *
 * <h2>Threading model (the important part)</h2>
 * Everything that touches Raft state runs on <b>one</b> single-threaded executor - the "Raft thread".
 * Incoming RPCs ({@link #receiveRequestVote}/{@link #receiveAppendEntries}) and outgoing RPC replies
 * are all marshalled onto it, and timers fire on it too. Network I/O happens off-thread via the
 * async {@link RaftTransport}. This actor model eliminates almost all of the data races that make
 * consensus code notoriously hard to get right.
 *
 * <h2>Log indexing</h2>
 * Entries are 1-based: log index {@code i} is stored at {@code log.get(i-1)}. Index 0 is the virtual
 * empty entry (term 0).
 */
public final class RaftNode {
    private static final Log LOG = Log.of(RaftNode.class);

    private final int selfId;
    private final List<Integer> peers;
    private final int clusterSize;
    private final RaftTransport transport;
    private final StateMachine stateMachine;
    private final RaftPersistence persistence;
    private final RaftConfig config;
    private final RaftListener listener;
    private final ScheduledExecutorService exec;

    // -- Raft state (only touched on the Raft thread) --
    private long currentTerm;
    private int votedFor = -1;              // -1 = none
    private List<LogEntry> log = new ArrayList<>();
    private long commitIndex;
    private long lastApplied;
    private RaftRole role = RaftRole.FOLLOWER;
    private int leaderId = -1;
    private Set<Integer> votesReceived = new HashSet<>();
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private long electionDeadlineNanos;
    private final Map<Long, CompletableFuture<Long>> pendingProposals = new HashMap<>();

    /** Last time (epoch millis) each peer answered an AppendEntries - the leader's liveness view. */
    // Thread-safe so the controller can read it off the Raft thread.
    private final Map<Integer, Long> peerLastContactMillis = new ConcurrentHashMap<>();

    // -- snapshots readable from any thread --
    private volatile RaftRole roleSnapshot = RaftRole.FOLLOWER;
    private volatile long currentTermSnapshot;
    private volatile int leaderIdSnapshot = -1;

    public RaftNode(int selfId, List<Integer> peers, RaftTransport transport, StateMachine stateMachine,
                    RaftPersistence persistence, RaftConfig config, RaftListener listener) {
        this.selfId = selfId;
        this.peers = List.copyOf(peers);
        this.clusterSize = peers.size() + 1;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.persistence = persistence;
        this.config = config;
        this.listener = listener;
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mk-raft-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        exec.execute(() -> {
            PersistentState st = persistence.load();
            currentTerm = st.currentTerm;
            votedFor = st.votedFor;
            log = new ArrayList<>(st.log);
            role = RaftRole.FOLLOWER;
            leaderId = -1;
            resetElectionDeadline();
            updateSnapshotsAndNotify();
            LOG.info("raft node {} started (term={}, logSize={})", selfId, currentTerm, log.size());
        });
        exec.scheduleAtFixedRate(this::safeTick, config.tickMs, config.tickMs, TimeUnit.MILLISECONDS);
        exec.scheduleAtFixedRate(this::safeHeartbeat, config.heartbeatMs, config.heartbeatMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        submit(this::failPendingProposals);
        exec.shutdown();
        try {
            if (!exec.awaitTermination(1, TimeUnit.SECONDS)) {
                exec.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exec.shutdownNow();
        }
    }

    // -- public API (thread-safe) --

    /**
     * Proposes a command. The returned future completes with the entry's log index once it is
     * committed and applied, or completes exceptionally with {@link NotLeaderException} if this node
     * isn't the leader.
     */
    public CompletableFuture<Long> propose(byte[] command) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            exec.execute(() -> {
                if (role != RaftRole.LEADER) {
                    future.completeExceptionally(new NotLeaderException(leaderId));
                    return;
                }
                long index = lastLogIndex() + 1;
                log.add(new LogEntry(index, currentTerm, command));
                persist();
                pendingProposals.put(index, future);
                for (int peer : peers) {
                    sendAppendEntriesTo(peer);
                }
                advanceCommit(); // handles the single-node cluster case immediately
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(new IllegalStateException("raft node stopped"));
        }
        return future;
    }

    public CompletableFuture<RequestVoteResponse> receiveRequestVote(RequestVoteRequest req) {
        CompletableFuture<RequestVoteResponse> f = new CompletableFuture<>();
        try {
            exec.execute(() -> f.complete(handleRequestVote(req)));
        } catch (RejectedExecutionException e) {
            f.completeExceptionally(new IllegalStateException("raft node stopped"));
        }
        return f;
    }

    public CompletableFuture<AppendEntriesResponse> receiveAppendEntries(AppendEntriesRequest req) {
        CompletableFuture<AppendEntriesResponse> f = new CompletableFuture<>();
        try {
            exec.execute(() -> f.complete(handleAppendEntries(req)));
        } catch (RejectedExecutionException e) {
            f.completeExceptionally(new IllegalStateException("raft node stopped"));
        }
        return f;
    }

    public int id() {
        return selfId;
    }

    public RaftRole role() {
        return roleSnapshot;
    }

    public boolean isLeader() {
        return roleSnapshot == RaftRole.LEADER;
    }

    public long currentTerm() {
        return currentTermSnapshot;
    }

    public int leaderId() {
        return leaderIdSnapshot;
    }

    /** Epoch-millis of the last AppendEntries reply from {@code peer}, or 0 if never heard from. */
    public long lastContactMillis(int peer) {
        return peerLastContactMillis.getOrDefault(peer, 0L);
    }

    // -- timers (Raft thread) --

    private void safeTick() {
        try {
            if (role != RaftRole.LEADER && System.nanoTime() >= electionDeadlineNanos) {
                startElection();
            }
        } catch (RuntimeException e) {
            LOG.warn("raft {} tick error: {}", selfId, e.toString());
        }
    }

    private void safeHeartbeat() {
        try {
            if (role == RaftRole.LEADER) {
                for (int peer : peers) {
                    sendAppendEntriesTo(peer);
                }
            }
        } catch (RuntimeException e) {
            LOG.warn("raft {} heartbeat error: {}", selfId, e.toString());
        }
    }

    // -- elections --

    private void startElection() {
        currentTerm++;
        role = RaftRole.CANDIDATE;
        votedFor = selfId;
        votesReceived = new HashSet<>();
        votesReceived.add(selfId);
        leaderId = -1;
        persist();
        resetElectionDeadline();
        updateSnapshotsAndNotify();
        LOG.info("raft {} starting election for term {}", selfId, currentTerm);

        if (isMajority(votesReceived.size())) { // single-node cluster
            becomeLeader();
            return;
        }
        long term = currentTerm;
        long lastIndex = lastLogIndex();
        long lastTerm = lastLogTerm();
        for (int peer : peers) {
            RequestVoteRequest req = new RequestVoteRequest(term, selfId, lastIndex, lastTerm);
            transport.requestVote(peer, req,
                    (from, resp) -> submit(() -> onVoteResponse(term, from, resp)));
        }
    }

    private void onVoteResponse(long termWhenSent, int from, RequestVoteResponse resp) {
        if (resp == null) {
            return; // RPC failed; ignore
        }
        if (resp.term > currentTerm) {
            stepDown(resp.term, -1);
            persist();
            return;
        }
        if (role != RaftRole.CANDIDATE || currentTerm != termWhenSent) {
            return; // stale response
        }
        if (resp.voteGranted) {
            votesReceived.add(from);
            if (isMajority(votesReceived.size())) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = selfId;
        nextIndex.clear();
        matchIndex.clear();
        long next = lastLogIndex() + 1;
        for (int peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        updateSnapshotsAndNotify();
        LOG.info("raft {} became LEADER for term {}", selfId, currentTerm);
        for (int peer : peers) {
            sendAppendEntriesTo(peer); // assert leadership immediately
        }
    }

    // -- log replication (leader side) --

    private void sendAppendEntriesTo(int peer) {
        long term = currentTerm;
        long ni = nextIndex.getOrDefault(peer, lastLogIndex() + 1);
        long prevIndex = ni - 1;
        long prevTerm = termAt(prevIndex);
        List<LogEntry> entries = new ArrayList<>();
        for (long i = ni; i <= lastLogIndex(); i++) {
            entries.add(entryAt(i));
        }
        int numEntries = entries.size();
        AppendEntriesRequest req = new AppendEntriesRequest(term, selfId, prevIndex, prevTerm, entries, commitIndex);
        transport.appendEntries(peer, req,
                (from, resp) -> submit(() -> onAppendResponse(term, from, prevIndex, numEntries, resp)));
    }

    private void onAppendResponse(long termWhenSent, int from, long prevIndex, int numEntries,
                                  AppendEntriesResponse resp) {
        if (resp == null) {
            return;
        }
        peerLastContactMillis.put(from, System.currentTimeMillis()); // peer answered => it's alive
        if (resp.term > currentTerm) {
            stepDown(resp.term, -1);
            persist();
            return;
        }
        if (role != RaftRole.LEADER || currentTerm != termWhenSent) {
            return; // stale
        }
        if (resp.success) {
            long newMatch = prevIndex + numEntries;
            if (newMatch > matchIndex.getOrDefault(from, 0L)) {
                matchIndex.put(from, newMatch);
            }
            nextIndex.put(from, matchIndex.get(from) + 1);
            advanceCommit();
        } else {
            long backoff = resp.conflictIndex > 0
                    ? resp.conflictIndex
                    : Math.max(1, nextIndex.getOrDefault(from, 1L) - 1);
            nextIndex.put(from, Math.max(1, backoff));
            sendAppendEntriesTo(from); // retry further back
        }
    }

    private void advanceCommit() {
        long newCommit = commitIndex;
        for (long n = commitIndex + 1; n <= lastLogIndex(); n++) {
            // Raft safety: a leader only counts replicas to commit entries from its OWN term.
            if (termAt(n) != currentTerm) {
                continue;
            }
            int count = 1; // self
            for (int peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (isMajority(count)) {
                newCommit = n;
            }
        }
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
            completeProposals();
        }
    }

    // -- RPC handlers (Raft thread) --

    private RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term > currentTerm) {
            stepDown(req.term, -1);
        }
        boolean grant = false;
        if (req.term == currentTerm
                && (votedFor == -1 || votedFor == req.candidateId)
                && candidateLogUpToDate(req.lastLogIndex, req.lastLogTerm)) {
            votedFor = req.candidateId;
            grant = true;
            resetElectionDeadline();
        }
        persist();
        return new RequestVoteResponse(currentTerm, grant);
    }

    private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, 0, 0); // reject stale leader
        }
        boolean termChanged = false;
        if (req.term > currentTerm) {
            currentTerm = req.term;
            votedFor = -1;
            termChanged = true;
        }
        boolean wasLeader = role == RaftRole.LEADER;
        role = RaftRole.FOLLOWER;
        leaderId = req.leaderId;
        if (wasLeader) {
            failPendingProposals();
        }
        updateSnapshotsAndNotify();
        resetElectionDeadline();

        // Consistency check at prevLogIndex.
        if (req.prevLogIndex > lastLogIndex()) {
            if (termChanged) {
                persist();
            }
            return new AppendEntriesResponse(currentTerm, false, 0, lastLogIndex() + 1);
        }
        if (req.prevLogIndex > 0 && termAt(req.prevLogIndex) != req.prevLogTerm) {
            long conflictTerm = termAt(req.prevLogIndex);
            long conflictIndex = firstIndexOfTerm(conflictTerm, req.prevLogIndex);
            truncateFrom(req.prevLogIndex);
            persist();
            return new AppendEntriesResponse(currentTerm, false, 0, conflictIndex);
        }

        // Append new entries, truncating on the first conflict.
        long idx = req.prevLogIndex;
        boolean logChanged = false;
        for (LogEntry e : req.entries) {
            idx++;
            if (idx <= lastLogIndex()) {
                if (termAt(idx) != e.term) {
                    truncateFrom(idx);
                    log.add(e);
                    logChanged = true;
                }
                // else: already have a matching entry, skip
            } else {
                log.add(e);
                logChanged = true;
            }
        }
        if (termChanged || logChanged) {
            persist();
        }

        if (req.leaderCommit > commitIndex) {
            commitIndex = Math.min(req.leaderCommit, lastLogIndex());
            applyCommitted();
        }
        return new AppendEntriesResponse(currentTerm, true, req.prevLogIndex + req.entries.size(), 0);
    }

    // -- helpers --

    private void stepDown(long term, int newLeaderId) {
        boolean wasLeader = role == RaftRole.LEADER;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = -1;
        }
        role = RaftRole.FOLLOWER;
        leaderId = newLeaderId;
        if (wasLeader) {
            failPendingProposals();
        }
        updateSnapshotsAndNotify();
        resetElectionDeadline();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = entryAt(lastApplied);
            try {
                stateMachine.apply(e.index, e.command);
            } catch (RuntimeException ex) {
                LOG.warn("raft {} state machine failed to apply index {}: {}", selfId, e.index, ex.toString());
            }
        }
    }

    private void completeProposals() {
        Iterator<Map.Entry<Long, CompletableFuture<Long>>> it = pendingProposals.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, CompletableFuture<Long>> entry = it.next();
            if (entry.getKey() <= commitIndex) {
                entry.getValue().complete(entry.getKey());
                it.remove();
            }
        }
    }

    private void failPendingProposals() {
        for (CompletableFuture<Long> f : pendingProposals.values()) {
            f.completeExceptionally(new NotLeaderException(leaderId));
        }
        pendingProposals.clear();
    }

    private boolean candidateLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastLogTerm();
        long myLastIndex = lastLogIndex();
        return candidateLastTerm > myLastTerm
                || (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    }

    private long firstIndexOfTerm(long term, long upto) {
        for (long i = 1; i <= upto; i++) {
            if (termAt(i) == term) {
                return i;
            }
        }
        return upto;
    }

    private void truncateFrom(long index) {
        if (index <= log.size()) {
            log.subList((int) (index - 1), log.size()).clear();
        }
    }

    private long lastLogIndex() {
        return log.size();
    }

    private long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term;
    }

    private long termAt(long index) {
        if (index <= 0 || index > log.size()) {
            return 0;
        }
        return log.get((int) (index - 1)).term;
    }

    private LogEntry entryAt(long index) {
        return log.get((int) (index - 1));
    }

    private boolean isMajority(int count) {
        return count * 2 > clusterSize;
    }

    private void resetElectionDeadline() {
        long span = config.electionMaxMs - config.electionMinMs + 1;
        long timeoutMs = config.electionMinMs + ThreadLocalRandom.current().nextLong(span);
        electionDeadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
    }

    private void persist() {
        persistence.save(new PersistentState(currentTerm, votedFor, new ArrayList<>(log)));
    }

    private void updateSnapshotsAndNotify() {
        if (roleSnapshot != role || currentTermSnapshot != currentTerm || leaderIdSnapshot != leaderId) {
            roleSnapshot = role;
            currentTermSnapshot = currentTerm;
            leaderIdSnapshot = leaderId;
            if (listener != null) {
                try {
                    listener.onRoleChange(role, currentTerm, leaderId);
                } catch (RuntimeException e) {
                    // a listener failure must never break Raft
                }
            }
        }
    }

    private void submit(Runnable task) {
        try {
            exec.execute(task);
        } catch (RejectedExecutionException ignored) {
            // node stopped; drop the callback
        }
    }
}