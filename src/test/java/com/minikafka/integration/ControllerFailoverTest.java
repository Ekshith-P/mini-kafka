package com.minikafka.integration;

import com.minikafka.broker.Broker;
import com.minikafka.broker.BrokerConfig;
import com.minikafka.client.Consumer;
import com.minikafka.client.KafkaClient;
import com.minikafka.client.Producer;
import com.minikafka.protocol.messages.MetadataResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boots a real 3-broker Raft cluster in-process, kills the leader of a partition, and asserts that
 * the controller elects a new leader and no {@code acks=all} data is lost. This is the end-to-end
 * proof of the KRaft-style failover path. It's a slow integration test (tens of seconds).
 */
class ControllerFailoverTest {

    @Test
    void leaderFailsOverWhenABrokerDies(@TempDir Path base) throws Exception {
        int[] ports = new int[3];
        for (int i = 0; i < 3; i++) {
            try (ServerSocket s = new ServerSocket(0)) {
                ports[i] = s.getLocalPort();
            }
        }
        String clusterBrokers = "1:localhost:" + ports[0] + ",2:localhost:" + ports[1] + ",3:localhost:" + ports[2];
        String bootstrap = "localhost:" + ports[0] + ",localhost:" + ports[1] + ",localhost:" + ports[2];

        Broker[] brokers = new Broker[3];
        for (int i = 0; i < 3; i++) {
            Properties p = new Properties();
            p.setProperty("broker.id", String.valueOf(i + 1));
            p.setProperty("listeners.host", "localhost");
            p.setProperty("listeners.port", String.valueOf(ports[i]));
            p.setProperty("log.dirs", base.resolve("broker-" + (i + 1)).toString());
            p.setProperty("cluster.brokers", clusterBrokers);
            p.setProperty("topics", "orders:3:3");
            p.setProperty("raft.election.min.ms", "250");
            p.setProperty("raft.election.max.ms", "500");
            p.setProperty("raft.heartbeat.ms", "60");
            p.setProperty("raft.liveness.timeout.ms", "1200");
            brokers[i] = new Broker(BrokerConfig.fromProperties(p));
        }
        for (Broker b : brokers) {
            b.start();
        }
        try {
            Thread.sleep(3000); // let raft elect a controller and replicas catch up

            try (Producer producer = new Producer(bootstrap, (short) -1)) { // acks=all
                for (int i = 0; i < 30; i++) {
                    producer.send("orders", "k" + i, "v" + i);
                }
            }
            assertEquals(30, consumeAll(bootstrap, 30, 8000).size(), "should read all 30 before failover");

            int oldLeader = leaderOf(bootstrap, 0);
            assertTrue(oldLeader >= 1, "partition 0 should have a leader");
            brokers[oldLeader - 1].close(); // kill the leader of orders-0

            int newLeader = awaitNewLeader(0, oldLeader, bootstrap, 15000);
            assertTrue(newLeader >= 1, "a new leader should be elected for orders-0");
            assertNotEquals(oldLeader, newLeader, "leadership must move off the dead broker");

            try (Producer producer = new Producer(bootstrap, (short) -1)) {
                for (int i = 0; i < 30; i++) {
                    producer.send("orders", "k2-" + i, "v2-" + i);
                }
            }

            Set<String> all = consumeAll(bootstrap, 60, 12000);
            assertEquals(60, all.size(), "all records (pre + post failover) must be durable");
            for (int i = 0; i < 30; i++) {
                assertTrue(all.contains("v" + i) && all.contains("v2-" + i), "missing record " + i);
            }
        } finally {
            for (Broker b : brokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // already closed
                }
            }
        }
    }

    private static int leaderOf(String bootstrap, int partition) throws Exception {
        try (KafkaClient c = new KafkaClient(bootstrap, "probe", 3000)) {
            MetadataResponse md = c.fetchMetadata(List.of("orders"));
            return md.leaderFor("orders", partition);
        }
    }

    private static int awaitNewLeader(int partition, int oldLeader, String bootstrap, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                int l = leaderOf(bootstrap, partition);
                if (l >= 1 && l != oldLeader) {
                    return l;
                }
            } catch (Exception ignored) {
                // cluster still settling
            }
            sleep(300);
        }
        return -1;
    }

    private static Set<String> consumeAll(String bootstrap, int expect, long timeoutMs) throws Exception {
        Set<String> got = new HashSet<>();
        try (Consumer c = new Consumer(bootstrap, "grp-" + System.nanoTime(), true)) {
            c.subscribe("orders");
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (got.size() < expect && System.currentTimeMillis() < deadline) {
                for (Consumer.ConsumerRecord r : c.poll()) {
                    got.add(r.valueAsString());
                }
                if (got.size() < expect) {
                    sleep(80);
                }
            }
        }
        return got;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}