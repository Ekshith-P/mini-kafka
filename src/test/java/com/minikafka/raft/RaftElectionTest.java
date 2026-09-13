package com.minikafka.raft;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftElectionTest {

    private static TestNetwork threeNodeCluster() {
        TestNetwork net = new TestNetwork();
        List<Integer> ids = List.of(1, 2, 3);
        for (int id : ids) {
            List<Integer> peers = ids.stream().filter(x -> x != id).collect(Collectors.toList());
            net.register(new RaftNode(id, peers, net.transportFor(id), new RecordingStateMachine(),
                    new InMemoryPersistence(), RaftConfig.defaults(), null));
        }
        return net;
    }

    @Test
    void electsExactlyOneLeader() throws Exception {
        TestNetwork net = threeNodeCluster();
        net.startAll();
        try {
            RaftNode leader = net.awaitLeader(3000);
            assertNotNull(leader, "a leader should be elected within 3s");
            Thread.sleep(300); // let the cluster settle
            assertEquals(1, net.connectedLeaderCount(), "there must be exactly one leader");
        } finally {
            net.stopAll();
        }
    }

    @Test
    void reelectsAfterLeaderFailure() throws Exception {
        TestNetwork net = threeNodeCluster();
        net.startAll();
        try {
            RaftNode first = net.awaitLeader(3000);
            assertNotNull(first, "an initial leader should be elected");
            long termBefore = first.currentTerm();

            net.disconnect(first.id()); // simulate the leader crashing

            RaftNode second = net.awaitLeaderExcluding(Set.of(first.id()), 3000);
            assertNotNull(second, "a new leader should emerge among the surviving majority");
            assertNotEquals(first.id(), second.id(), "the failed node must not remain leader");
            assertTrue(second.currentTerm() > termBefore,
                    "the new leader's term must exceed the old leader's term");
        } finally {
            net.stopAll();
        }
    }
}
