package com.minikafka.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RaftReplicationTest {

    @Test
    void replicatesCommittedCommandsToEveryNode() throws Exception {
        TestNetwork net = new TestNetwork();
        List<Integer> ids = List.of(1, 2, 3);
        Map<Integer, RecordingStateMachine> stateMachines = new HashMap<>();
        for (int id : ids) {
            RecordingStateMachine sm = new RecordingStateMachine();
            stateMachines.put(id, sm);
            List<Integer> peers = ids.stream().filter(x -> x != id).collect(Collectors.toList());
            net.register(new RaftNode(id, peers, net.transportFor(id), sm,
                    new InMemoryPersistence(), RaftConfig.defaults(), null));
        }
        net.startAll();
        try {
            RaftNode leader = net.awaitLeader(3000);
            assertNotNull(leader);

            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                futures.add(leader.propose(("cmd" + i).getBytes(StandardCharsets.UTF_8)));
            }
            for (CompletableFuture<Long> f : futures) {
                f.get(3, TimeUnit.SECONDS); // completes only once committed
            }

            List<String> expected = List.of("cmd0", "cmd1", "cmd2", "cmd3", "cmd4");
            for (int id : ids) {
                awaitApplied(stateMachines.get(id), 5, 3000);
                assertEquals(expected, stateMachines.get(id).applied(),
                        "node " + id + " should have applied every command in order");
            }
        } finally {
            net.stopAll();
        }
    }

    @Test
    void singleNodeClusterCommitsImmediately() throws Exception {
        TestNetwork net = new TestNetwork();
        RecordingStateMachine sm = new RecordingStateMachine();
        net.register(new RaftNode(1, List.of(), net.transportFor(1), sm,
                new InMemoryPersistence(), RaftConfig.defaults(), null));
        net.startAll();
        try {
            RaftNode leader = net.awaitLeader(2000);
            assertNotNull(leader);
            long index = leader.propose("only".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
            assertEquals(1, index);
            awaitApplied(sm, 1, 1000);
            assertEquals(List.of("only"), sm.applied());
        } finally {
            net.stopAll();
        }
    }

    private static void awaitApplied(RecordingStateMachine sm, int count, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (sm.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }
}

