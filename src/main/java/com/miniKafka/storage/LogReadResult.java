package com.minikafka.storage;

import java.util.Collections;
import java.util.List;

/** The result of reading from a {@link Log}: the records plus the offsets a consumer needs. */
public final class LogReadResult {
    public final List<Record> records;
    public final long highWatermark;
    public final long logStartOffset;
    public final short errorCode;

    public LogReadResult(List<Record> records, long highWatermark,
                         long logStartOffset, short errorCode) {
        this.records = records;
        this.highWatermark = highWatermark;
        this.logStartOffset = logStartOffset;
        this.errorCode = errorCode;
    }

    public static LogReadResult error(short errorCode, long highWatermark,
                                      long logStartOffset) {
        return new LogReadResult(
                Collections.emptyList(),
                highWatermark,
                logStartOffset,
                errorCode
        );
    }
}