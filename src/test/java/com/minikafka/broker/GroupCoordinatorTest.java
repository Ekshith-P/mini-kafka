package com.minikafka.broker;

import com.minikafka.common.Errors;
import com.minikafka.common.Node;
import com.minikafka.protocol.messages.HeartbeatResponse;
import com.minikafka.protocol.messages.JoinGroupRequest;
import com.minikafka.protocol.messages.JoinGroupResponse;
import com.minikafka.protocol.messages.LeaveGroupRequest;
import com.minikafka.protocol.messages.SyncGroupRequest;
import com.minikafka.protocol.messages.SyncGroupResponse;
import com.minikafka.protocol.messages.HeartbeatRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupCoordinatorTest {

    private GroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        Cluster cluster = new Cluster(List.of(new Node(1, "h", 1)));
        cluster.addOrUpdateTopic(new TopicConfig("t", 3, 1));
        coordinator = new GroupCoordinator(cluster, 2000);
        coordinator.start();
    }

    @AfterEach
    void tearDown() {
        coordinator.stop();
    }

    private static JoinGroupRequest join(String memberId) {
        return new JoinGroupRequest("G", memberId, 6000, Collections.singletonList("t"));
    }

    private static Set<Integer> partitionsOf(SyncGroupResponse resp) {
        Set<Integer> partitions = new HashSet<>();
        for (SyncGroupResponse.Assignment a : resp.assignments) {
            for (int p : a.partitions) {
                partitions.add(p);
            }
        }
        return partitions;
    }

    @Test
    void twoMembersSplitPartitionsDisjointlyInTheSameGeneration() throws Exception {
        // First member joins alone -> gets everything at generation 1.
        JoinGroupResponse first = coordinator.joinGroup(join(""));
        assertEquals(Errors.NONE, first.errorCode);

        // Second member joins; its reply is held on the barrier until the first rejoins.
        CompletableFuture<JoinGroupResponse> secondFuture =
                CompletableFuture.supplyAsync(() -> coordinator.joinGroup(join("")));
        Thread.sleep(200); // let the second member register and open the rebalance

        // First member rejoins (as a real consumer would after a heartbeat) -> barrier completes.
        JoinGroupResponse firstRejoin = coordinator.joinGroup(join(first.memberId));
        JoinGroupResponse second = secondFuture.get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NONE, second.errorCode);
        assertEquals(firstRejoin.generationId, second.generationId, "both members share one generation");

        SyncGroupResponse s1 = coordinator.syncGroup(
                new SyncGroupRequest("G", firstRejoin.memberId, firstRejoin.generationId));
        SyncGroupResponse s2 = coordinator.syncGroup(
                new SyncGroupRequest("G", second.memberId, second.generationId));
        Set<Integer> p1 = partitionsOf(s1);
        Set<Integer> p2 = partitionsOf(s2);

        assertTrue(Collections.disjoint(p1, p2), "assignments must not overlap");
        assertTrue(!p1.isEmpty() && !p2.isEmpty(), "both members get some partitions");
        Set<Integer> all = new HashSet<>(p1);
        all.addAll(p2);
        assertEquals(Set.of(0, 1, 2), all, "every partition is assigned exactly once");
    }

    @Test
    void heartbeatWithStaleGenerationAsksMemberToRejoin() {
        JoinGroupResponse joined = coordinator.joinGroup(join(""));
        HeartbeatResponse ok = coordinator.heartbeat(
                new HeartbeatRequest("G", joined.memberId, joined.generationId));
        assertEquals(Errors.NONE, ok.errorCode);

        HeartbeatResponse stale = coordinator.heartbeat(
                new HeartbeatRequest("G", joined.memberId, joined.generationId - 1));
        assertEquals(Errors.ILLEGAL_GENERATION, stale.errorCode);

        HeartbeatResponse unknown = coordinator.heartbeat(new HeartbeatRequest("G", "nope", 1));
        assertEquals(Errors.UNKNOWN_MEMBER_ID, unknown.errorCode);
    }

    @Test
    void leaveGroupRemovesTheMember() {
        JoinGroupResponse joined = coordinator.joinGroup(join(""));
        coordinator.leaveGroup(new LeaveGroupRequest("G", joined.memberId));
        HeartbeatResponse afterLeave = coordinator.heartbeat(
                new HeartbeatRequest("G", joined.memberId, joined.generationId));
        assertEquals(Errors.UNKNOWN_MEMBER_ID, afterLeave.errorCode);
    }
}