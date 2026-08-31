package com.minikafka.replication;

import com.minikafka.client.BrokerConnection;
import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.FetchRequest;
import com.minikafka.protocol.messages.FetchResponse;
import com.minikafka.storage.Log;
import com.minikafka.storage.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs on a follower broker: continuously fetches new records for one partition from its leader and
 * appends them to the local log — the follower side of Kafka's pull-based replication.
 *
 * <p>Each round it asks the Leader for everything from its own log-end offset onward (tagging the
 * request with its broker id so the leader can track how far it has replicated), appends whatever
 * comes back, and advances its local high-watermark to the leader's (never past what it actually
 * holds). If the leader is unreachable it reconnects and retries with a short backoff.
 */
final class ReplicaFetcher {
    private static final com.minikafka.common.Log LOG = com.minikafka.common.Log.of(ReplicaFetcher.class);

    private final TopicPartition tp;
    private final Node leader;
    private final Log log;
    private final int brokerId;
    private final int fetchIntervalMs;
    private final int fetchMaxBytes;

    private volatile boolean running;
    private Thread thread;
    private BrokerConnection connection;

    ReplicaFetcher(TopicPartition tp, Node leader, Log log, int brokerId,
                   int fetchIntervalMs, int fetchMaxBytes) {
        this.tp = tp;
        this.leader = leader;
        this.log = log;
        this.brokerId = brokerId;
        this.fetchIntervalMs = fetchIntervalMs;
        this.fetchMaxBytes = fetchMaxBytes;
    }

    int leaderId() { return leader.id(); }

    void start() {
        running = true;
        thread = new Thread(this::run, "mk-replica-fetcher-" + tp + "->b" + leader.id());
        thread.setDaemon(true);
        thread.start();
        LOG.info("started replica fetcher for {} from leader {}", tp, leader);
    }

    private void run() {
        while (running) {
            try {
                boolean gotRecords = fetchOnce();
                if (!gotRecords) {
                    Thread.sleep(fetchIntervalMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                LOG.warn("replica fetch for {} failed: {}; will reconnect", tp, e.toString());
                closeConnection();
                sleepQuietly(Math.max(fetchIntervalMs, 500));
            } catch (RuntimeException e) {
                LOG.error("unexpected error replicating " + tp, e);
                sleepQuietly(Math.max(fetchIntervalMs, 500));
            }
        }
        closeConnection();
    }

    /** @return true if at least one record was appended (so the caller should poll again immediately) */
    private boolean fetchOnce() throws IOException {
        if (connection == null) {
            connection = new BrokerConnection(leader.host(), leader.port(), 5000);
        }

        long fetchOffset = log.logEndOffset();

        FetchRequest.Partition part = new FetchRequest.Partition(tp.partition(), fetchOffset, fetchMaxBytes);
        FetchRequest.Topic topic = new FetchRequest.Topic(tp.topic(), Collections.singletonList(part));
        FetchRequest request = new FetchRequest(brokerId, 0, 1, Collections.singletonList(topic));

        ByteWriter body = new ByteWriter();
        request.writeTo(body);

        ByteBuffer respBuf = connection.send(ApiKeys.FETCH.id, (short) 0, "replica-" + brokerId, body.toByteArray());
        FetchResponse response = FetchResponse.parse(respBuf);

        FetchResponse.Partition data = findPartition(response);
        if (data == null) {
            return false;
        }
        if (data.errorCode != Errors.NONE) {
            LOG.warn("leader returned {} for {} at offset {}", Errors.message(data.errorCode), tp, fetchOffset);
            return false;
        }

        if (!data.records.isEmpty()) {
            List<Record> records = new ArrayList<>(data.records.size());
            for (FetchResponse.Record r : data.records) {
                records.add(new Record(r.offset, r.timestamp, r.key, r.value));
            }
            log.appendAsFollower(records);
        }
        // Advance our high-watermark to the Leader's, capped at what we actually hold.
        log.updateHighWatermark(data.highWatermark);
        return !data.records.isEmpty();
    }

    private FetchResponse.Partition findPartition(FetchResponse response) {
        for (FetchResponse.Topic t : response.topics) {
            if (t.name.equals(tp.topic())) {
                for (FetchResponse.Partition p : t.partitions) {
                    if (p.index == tp.partition()) {
                        return p;
                    }
                }
            }
        }
        return null;
    }

    void shutdown() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
        closeConnection();
    }

    private void closeConnection() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}