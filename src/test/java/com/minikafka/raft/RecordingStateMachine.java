package com.minikafka.raft;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

/** Test state machine that just records the commands it applies, so tests can assert consistency. */
final class RecordingStateMachine implements StateMachine {
    private final List<String> applied = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void apply(long index, byte[] command) {
        applied.add(new String(command, StandardCharsets.UTF_8));
    }

    List<String> applied() {
        synchronized (applied) {
            return new ArrayList<>(applied);
        }
    }

    int size() {
        return applied.size();
    }
}
