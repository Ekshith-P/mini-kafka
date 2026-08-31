package com.minikafka.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight, thread-safe broker counters. Uses {@link LongAdder} so many handler threads can
 * increment without contending on a single hot field. A background reporter (see the broker) logs a
 * snapshot periodically - enough to compute throughput for benchmarks.
 */
public final class Metrics {
    private final LongAdder produceRequests = new LongAdder();
    private final LongAdder producedMessages = new LongAdder();
    private final LongAdder producedBytes = new LongAdder();
    private final LongAdder fetchRequests = new LongAdder();
    private final LongAdder fetchedMessages = new LongAdder();
    private final LongAdder activeConnections = new LongAdder();

    public void onProduce(int messages, long bytes) {
        produceRequests.increment();
        producedMessages.add(messages);
        producedBytes.add(bytes);
    }

    public void onFetch(int messages) {
        fetchRequests.increment();
        fetchedMessages.add(messages);
    }

    public void connectionOpened() {
        activeConnections.increment();
    }

    public void connectionClosed() {
        activeConnections.decrement();
    }

    public long producedMessages() {
        return producedMessages.sum();
    }

    public long producedBytes() {
        return producedBytes.sum();
    }

    public long fetchedMessages() {
        return fetchedMessages.sum();
    }

    public String snapshot() {
        return String.format(
                "produce[req=%d msgs=%d bytes=%d] fetch[req=%d msgs=%d] connections=%d",
                produceRequests.sum(), producedMessages.sum(), producedBytes.sum(),
                fetchRequests.sum(), fetchedMessages.sum(), activeConnections.sum());
    }
}