package com.minikafka.storage;

import com.minikafka.common.Errors;
import com.minikafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTest {

    private static final TopicPartition TP = new TopicPartition("t", 0);

    @Test
    void appendReadAndHighWatermark(@TempDir Path dir) {
        Log log = new Log(dir, TP, 1 << 20, 4096);
        assertEquals(0, log.append("k0".getBytes(), "v0".getBytes()));
        assertEquals(1, log.append(null, "v1".getBytes()));
        assertEquals(2, log.logEndOffset());
        assertEquals(2, log.highWatermark()); // single replica auto-advances HW to the LEO

        LogReadResult res = log.read(0, 1 << 20, false);
        assertEquals(Errors.NONE, res.errorCode);
        assertEquals(2, res.records.size());
        assertEquals("v0", res.records.get(0).valueAsString());
        assertNull(res.records.get(1).key);
        assertEquals("v1", res.records.get(1).valueAsString());
        log.close();
    }

    @Test
    void recoversAfterReopen(@TempDir Path dir) {
        Log log = new Log(dir, TP, 1 << 20, 4096);
        for (int i = 0; i < 5; i++) {
            log.append(null, ("v" + i).getBytes());
        }
        log.close();

        Log reopened = new Log(dir, TP, 1 << 20, 4096);
        assertEquals(5, reopened.logEndOffset());
        LogReadResult res = reopened.read(3, 1 << 20, false);
        assertEquals(2, res.records.size());
        assertEquals("v3", res.records.get(0).valueAsString());
        assertEquals("v4", res.records.get(1).valueAsString());
        reopened.close();
    }

    @Test
    void rollsSegmentsAndReadsAcrossThem(@TempDir Path dir) throws IOException {
        // Tiny segment size forces frequent rolling.
        Log log = new Log(dir, TP, 256, 64);
        int total = 100;
        for (int i = 0; i < total; i++) {
            log.append(null, ("message-number-" + i).getBytes());
        }
        assertEquals(total, log.logEndOffset());

        long segmentFiles;
        try (Stream<Path> files = Files.list(dir)) {
            segmentFiles = files.filter(p -> p.getFileName().toString().endsWith(".log")).count();
        }
        assertTrue(segmentFiles > 1, "expected multiple segments, got " + segmentFiles);

        // Read the whole log the way a consumer would: fetch, advance, repeat.
        List<Record> all = new ArrayList<>();
        long offset = 0;
        while (offset < log.logEndOffset()) {
            LogReadResult res = log.read(offset, 10_000, false);
            if (res.records.isEmpty()) {
                break;
            }
            all.addAll(res.records);
            offset = res.records.get(res.records.size() - 1).offset + 1;
        }
        assertEquals(total, all.size());
        assertEquals("message-number-0", all.get(0).valueAsString());
        assertEquals("message-number-99", all.get(99).valueAsString());
        log.close();
    }

    @Test
    void consumerCannotReadAboveHighWatermark(@TempDir Path dir) {
        Log log = new Log(dir, TP, 1 << 20, 4096);
        log.setAutoAdvanceHighWatermark(false); // simulate a replicated leader with no caught-up followers
        log.append(null, "v0".getBytes());
        log.append(null, "v1".getBytes());
        assertEquals(2, log.logEndOffset());
        assertEquals(0, log.highWatermark());

        // A consumer (forFollower=false) sees nothing until the HW advances.
        assertEquals(0, log.read(0, 1 << 20, false).records.size());
        // A follower (forFollower=true) may read up to the LEO.
        assertEquals(2, log.read(0, 1 << 20, true).records.size());

        log.updateHighWatermark(2);
        assertEquals(2, log.read(0, 1 << 20, false).records.size());
        log.close();
    }
}