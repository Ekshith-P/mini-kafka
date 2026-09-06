package com.minikafka.raft;

import com.minikafka.common.Log;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable Raft state on disk. The whole {@link PersistentState} is rewritten on every change
 * (the metadata log is small and changes rarely, so this is fine) and swapped atomically so a crash
 * mid-write can never leave a torn file. Restoring durable term/vote is what keeps Raft's "at most
 * one leader per term" guarantee across restarts.
 */
public final class FilePersistence implements RaftPersistence {

    private static final Log LOG = Log.of(FilePersistence.class);

    private final Path file;
    private final Path tmp;

    public FilePersistence(Path file) {
        this.file = file;
        this.tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            Files.createDirectories(file.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create raft state dir", e);
        }
    }

    @Override
    public synchronized void save(PersistentState state) {
        ByteWriter w = new ByteWriter();
        w.putLong(state.currentTerm);
        w.putInt(state.votedFor);
        w.putInt(state.log.size());

        for (LogEntry e : state.log) {
            w.putLong(e.index);
            w.putLong(e.term);
            w.putBytes(e.command);
        }

        try {
            Files.write(tmp, w.toByteArray());
            try {
                Files.move(
                        tmp,
                        file,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                );
            } catch (IOException atomicUnsupported) {
                Files.move(
                        tmp,
                        file,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist raft state to " + file, e);
        }
    }

    @Override
    public PersistentState load() {
        if (!Files.exists(file)) {
            return PersistentState.empty();
        }

        try {
            ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file));

            long term = b.getLong();
            int voterFor = b.getInt();
            int count = b.getInt();

            List<LogEntry> log =
                    new ArrayList<>(Math.max(0, count));

            for (int i = 0; i < count; i++) {
                long index = b.getLong();
                long entryTerm = b.getLong();
                byte[] command = Protocol.getBytes(b);

                log.add(new LogEntry(index, entryTerm, command));
            }

            return new PersistentState(term, voterFor, log);

        } catch (RuntimeException | IOException e) {
            LOG.warn(
                    "could not read raft state from {} (starting fresh): {}",
                    file,
                    e.toString()
            );
            return PersistentState.empty();
        }
    }
}